//! [CONTROL PLANE] Per-connection subscription state, owned by the connection thread: sent,
//! pending, confirmed; a feed is never confirmed from local send success alone.
//!
//! - Every feed of the connection is subscribed on every session: the batched SUBSCRIBE
//!   frames are built once at startup and replayed on each new session.
//! - `active` = subscribe sent on this session (the decoder accepts data for these);
//!   `confirmed` = acknowledged by the venue (`FeedStatus::Up`). Unconfirmed feeds fail after
//!   `ACK_TIMEOUT` (`FeedStatus::Failed`). A failed feed's batch is sent again `RETRY_AFTER`
//!   later within the same session (and on every new session), so a rejected or lost
//!   subscription does not wait for the next reconnect (up to 24 h on Binance).
//! - A sliding-window limiter keeps requests under the venue limit. The clock is read only
//!   when something is queued or pending.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::control_plane::identity::FeedId;
use crate::control_plane::startup::resolver::{ConnectionPlan, StartupPlan};
use crate::data_plane::events::{EventOut, FeedStatus, MdEvent};
use crate::data_plane::feed_set::FeedSet;
use crate::venues::{Ack, Venue};

const ACK_TIMEOUT: Duration = Duration::from_secs(10);
/// Pause before a failed batch is sent again within the session.
const RETRY_AFTER: Duration = Duration::from_secs(30);

/// One pre-built SUBSCRIBE frame, its request id and the feeds it covers.
struct Batch {
    frame: Vec<u8>,
    id: u64,
    feeds: Vec<FeedId>,
}

pub struct Subscriptions {
    pub active: FeedSet,
    confirmed: FeedSet,
    pending_since: Vec<Option<Instant>>,
    pending_count: usize,
    batches: Vec<Batch>,
    /// Index of the next batch to send on this session (`batches.len()` = all sent).
    next_batch: usize,
    /// Feed -> index of the batch that subscribes it.
    batch_of: Vec<u32>,
    /// Per batch: when a failed batch is due to be sent again.
    retry_at: Vec<Option<Instant>>,
    retry_scheduled: usize,
    /// Due batches waiting for the rate limiter (sent after the initial batches).
    retry_queue: VecDeque<usize>,
    limiter: SlidingWindow,
}

impl Subscriptions {
    pub fn new<V: Venue>(plan: &StartupPlan, conn: &ConnectionPlan) -> Self {
        let n = plan.feeds.len();
        let batches: Vec<Batch> = conn
            .feeds
            .chunks(V::LIMITS.max_streams_per_request.max(1))
            .zip(1u64..)
            .map(|(chunk, id)| {
                let streams: Vec<&str> = chunk.iter().map(|&f| plan.feed(f).stream.as_str()).collect();
                Batch { frame: V::subscribe_request(&streams, id), id, feeds: chunk.to_vec() }
            })
            .collect();
        let mut batch_of = vec![u32::MAX; n];
        for (b, batch) in batches.iter().enumerate() {
            for f in &batch.feeds {
                batch_of[f.index()] = b as u32;
            }
        }
        // One request of headroom under the venue limit.
        let per_sec = V::LIMITS.max_requests_per_sec.saturating_sub(1).max(1) as usize;
        Self {
            batch_of,
            retry_at: vec![None; batches.len()],
            retry_scheduled: 0,
            retry_queue: VecDeque::with_capacity(batches.len()),
            active: FeedSet::new(n),
            confirmed: FeedSet::new(n),
            pending_since: vec![None; n],
            pending_count: 0,
            batches,
            next_batch: usize::MAX,
            limiter: SlidingWindow::new(per_sec, Duration::from_millis(1050)),
        }
    }

    /// New session: reset session state and queue every batch for sending.
    pub fn on_connected(&mut self) {
        self.reset_session();
        self.next_batch = 0;
    }

    /// Session ended: confirmed feeds go Down; everything session-scoped is cleared.
    pub fn on_disconnected(&mut self, out: &mut EventOut, tsc: u64) {
        for f in self.confirmed.iter() {
            out.emit(MdEvent::status(f, FeedStatus::Down, tsc));
        }
        self.reset_session();
        self.next_batch = usize::MAX;
    }

    fn reset_session(&mut self) {
        self.active.clear();
        self.confirmed.clear();
        self.pending_since.fill(None);
        self.pending_count = 0;
        self.retry_at.fill(None);
        self.retry_scheduled = 0;
        self.retry_queue.clear();
    }

    #[inline(always)]
    pub fn has_outbound(&self) -> bool {
        self.next_batch < self.batches.len() || !self.retry_queue.is_empty()
    }

    /// Something time-driven is outstanding: unacknowledged requests or scheduled retries.
    #[inline(always)]
    pub fn needs_timer(&self) -> bool {
        self.pending_count != 0 || self.retry_scheduled != 0
    }

    /// Next frame to send now, if any and if the rate limit allows.
    pub fn next_outbound(&mut self) -> Option<&[u8]> {
        if !self.has_outbound() {
            return None;
        }
        let now = Instant::now();
        if !self.limiter.try_acquire(now) {
            return None;
        }
        let i = if self.next_batch < self.batches.len() {
            self.next_batch += 1;
            self.next_batch - 1
        } else {
            self.retry_queue.pop_front()?
        };
        for k in 0..self.batches[i].feeds.len() {
            let f = self.batches[i].feeds[k];
            // On a retry, feeds of the batch that are already confirmed or pending stay as
            // they are (no second `Up`).
            if self.confirmed.contains(f) || self.pending_since[f.index()].is_some() {
                continue;
            }
            self.active.insert(f);
            self.pending_since[f.index()] = Some(now);
            self.pending_count += 1;
        }
        Some(&self.batches[i].frame)
    }

    /// Venue acknowledgement reported by the decoder. Cold path.
    pub fn on_ack(&mut self, ack: Ack, out: &mut EventOut, tsc: u64) {
        match ack {
            Ack::Subscribed(f) => self.confirm(f, out, tsc),
            Ack::RequestOk(id) => self.for_request(id, |s, f| s.confirm(f, out, tsc)),
            Ack::RequestFailed(id) => self.for_request(id, |s, f| s.fail(f, out, tsc)),
            Ack::Error => {}
        }
    }

    /// Amortized timer work. Cold path. Fails requests the venue has not confirmed within
    /// `ACK_TIMEOUT` and queues failed batches whose `RETRY_AFTER` pause is over.
    pub fn on_timer(&mut self, out: &mut EventOut, tsc: u64) {
        let now = Instant::now();
        if self.pending_count != 0 {
            for i in 0..self.pending_since.len() {
                if self.pending_since[i].is_some_and(|t| now.duration_since(t) >= ACK_TIMEOUT) {
                    self.fail(FeedId(i as u16), out, tsc);
                }
            }
        }
        if self.retry_scheduled != 0 {
            for b in 0..self.retry_at.len() {
                if self.retry_at[b].is_some_and(|t| now >= t) {
                    self.retry_at[b] = None;
                    self.retry_scheduled -= 1;
                    self.retry_queue.push_back(b);
                }
            }
        }
    }

    /// Applies `op` to every feed of the batch with request id `id` (venues that ack by id).
    fn for_request(&mut self, id: u64, mut op: impl FnMut(&mut Self, FeedId)) {
        let Some(i) = self.batches.iter().position(|b| b.id == id) else { return };
        for k in 0..self.batches[i].feeds.len() {
            let f = self.batches[i].feeds[k];
            op(self, f);
        }
    }

    fn clear_pending(&mut self, f: FeedId) -> bool {
        let was = self.pending_since[f.index()].take().is_some();
        if was {
            self.pending_count -= 1;
        }
        was
    }

    fn confirm(&mut self, f: FeedId, out: &mut EventOut, tsc: u64) {
        if self.clear_pending(f) {
            self.confirmed.insert(f);
            out.emit(MdEvent::status(f, FeedStatus::Up, tsc));
        }
    }

    /// Venue rejected `f` or did not confirm it in time: `Failed`, and its batch is scheduled
    /// to be sent again after `RETRY_AFTER` (once per batch, however many feeds failed).
    fn fail(&mut self, f: FeedId, out: &mut EventOut, tsc: u64) {
        if self.clear_pending(f) {
            self.active.remove(f);
            out.emit(MdEvent::status(f, FeedStatus::Failed, tsc));
            let b = self.batch_of[f.index()] as usize;
            if let Some(slot) = self.retry_at.get_mut(b)
                && slot.is_none()
                && !self.retry_queue.contains(&b)
            {
                *slot = Some(Instant::now() + RETRY_AFTER);
                self.retry_scheduled += 1;
            }
        }
    }
}

/// At most `n` acquisitions in any `window`.
struct SlidingWindow {
    times: Box<[Option<Instant>]>,
    next: usize,
    window: Duration,
}

impl SlidingWindow {
    fn new(n: usize, window: Duration) -> Self {
        Self { times: vec![None; n].into_boxed_slice(), next: 0, window }
    }

    fn try_acquire(&mut self, now: Instant) -> bool {
        if let Some(t) = self.times[self.next]
            && now.duration_since(t) < self.window
        {
            return false;
        }
        self.times[self.next] = Some(now);
        self.next = (self.next + 1) % self.times.len();
        true
    }
}
