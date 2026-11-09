//! [DATA PLANE] Normalized market-data events: the only thing that leaves a connection thread.
//! One event = one 64-byte cache line, written once into a per-connection SPSC ring.

use crate::data_plane::spsc::{Consumer, Producer};
use crate::control_plane::identity::FeedId;

/// Events per connection ring (64 B each -> 512 KiB). Sized so all rings stay cache-resident
/// (6 connections = 3 MiB vs 12 MiB L3, 1.25 MiB L2 per core): a ring that wraps through
/// DRAM-cold lines costs a cache miss on every event write. The consumer must keep up;
/// on overflow the event is dropped.
pub const MD_RING_CAP: usize = 1 << 13;

pub type MdProducer = Producer<MdEvent, MD_RING_CAP>;
pub type MdConsumer = Consumer<MdEvent, MD_RING_CAP>;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    /// px/qty = best bid, aux0/aux1 = best ask, seq = book update id.
    Quote = 1,
    /// px/qty = trade, seq = trade id, aux0 = transact time (µs), flag BUYER_MAKER.
    Trade = 2,
    /// One level of a book snapshot. seq = book update id. FIRST/LAST frame the snapshot.
    BookSnapshot = 3,
    /// One level of a book delta. seq = last update id, aux0 = first update id.
    BookDelta = 4,
    /// Feed lifecycle; aux0 = `FeedStatus` code. Consumers must drop book state on `Down`.
    FeedStatus = 5,
}

#[repr(i64)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedStatus {
    /// Subscription confirmed by the venue on the current session (data that follows is continuous).
    Up = 1,
    /// Session ended; data stopped and may have gaps.
    Down = 2,
    /// Venue rejected the subscription or did not confirm it in time. Retried on the next session.
    Failed = 4,
}

pub mod flags {
    pub const BID: u8 = 1 << 0;
    pub const ASK: u8 = 1 << 1;
    pub const FIRST: u8 = 1 << 2;
    pub const LAST: u8 = 1 << 3;
    pub const BUYER_MAKER: u8 = 1 << 4;
    /// Book message with no levels (still framed with FIRST|LAST).
    pub const EMPTY: u8 = 1 << 5;
}

/// Prices and quantities are venue mantissas; value = mantissa * 10^exponent.
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MdEvent {
    /// TSC when the socket read that carried this message returned.
    pub recv_tsc: u64,
    /// Exchange event time, µs since epoch.
    pub exch_ts_us: i64,
    pub seq: i64,
    pub px: i64,
    pub qty: i64,
    pub aux0: i64,
    pub aux1: i64,
    pub feed: FeedId,
    pub kind: EventKind,
    pub flags: u8,
    pub price_exp: i8,
    pub qty_exp: i8,
    _pad: [u8; 2],
}

const _: () = assert!(core::mem::size_of::<MdEvent>() == 64);

impl MdEvent {
    #[inline(always)]
    pub const fn new(feed: FeedId, kind: EventKind, recv_tsc: u64) -> Self {
        Self {
            recv_tsc,
            exch_ts_us: 0,
            seq: 0,
            px: 0,
            qty: 0,
            aux0: 0,
            aux1: 0,
            feed,
            kind,
            flags: 0,
            price_exp: 0,
            qty_exp: 0,
            _pad: [0; 2],
        }
    }

    #[inline]
    pub fn status(feed: FeedId, status: FeedStatus, recv_tsc: u64) -> Self {
        let mut e = Self::new(feed, EventKind::FeedStatus, recv_tsc);
        e.aux0 = status as i64;
        e
    }
}

/// Producer side owned by the connection thread. Never blocks: a full ring drops the event
/// (a full ring means the consumer is not keeping up). `emit` only writes;
/// `flush` publishes everything emitted since the last flush (once per message).
/// `mark` / `rewind` discard unpublished events, so a multi-event message (a book) is
/// published whole or not at all.
pub struct EventOut {
    producer: MdProducer,
}

impl EventOut {
    pub fn new(producer: MdProducer) -> Self {
        Self { producer }
    }

    /// `false` when the ring is full and the event was dropped.
    #[inline(always)]
    pub fn emit(&mut self, ev: MdEvent) -> bool {
        self.producer.write(ev).is_ok()
    }

    #[inline(always)]
    pub fn mark(&self) -> EventMark {
        EventMark(self.producer.mark())
    }

    /// Drops every event emitted since `mark` (they were never published).
    #[inline(always)]
    pub fn rewind(&mut self, mark: EventMark) {
        self.producer.rewind(mark.0);
    }

    #[inline(always)]
    pub fn flush(&mut self) {
        self.producer.publish();
    }
}

/// Write position in an `EventOut`, taken before a multi-event message.
#[derive(Debug, Clone, Copy)]
pub struct EventMark(usize);

/// Emits one book message as level events framed FIRST..LAST without knowing the level count
/// up front (needed for streamed text formats). Holds one event back so the last one can be
/// marked LAST; a message without levels becomes a single EMPTY|FIRST|LAST event.
/// All-or-nothing: `abort` (bad level) or a full ring rewinds every event of the book.
pub struct BookEmitter {
    base: MdEvent,
    held: Option<MdEvent>,
    count: u32,
    mark: EventMark,
    complete: bool,
}

impl BookEmitter {
    #[inline(always)]
    pub fn new(out: &EventOut, base: MdEvent) -> Self {
        Self { base, held: None, count: 0, mark: out.mark(), complete: true }
    }

    #[inline(always)]
    pub fn level(&mut self, out: &mut EventOut, side: u8, px: i64, qty: i64) {
        let mut e = self.base;
        e.px = px;
        e.qty = qty;
        e.flags = side | if self.count == 0 { flags::FIRST } else { 0 };
        if let Some(prev) = self.held.replace(e) {
            self.complete &= out.emit(prev);
        }
        self.count += 1;
    }

    /// Discards the book (e.g. a level failed to parse): nothing of it is published.
    #[inline(always)]
    pub fn abort(self, out: &mut EventOut) {
        out.rewind(self.mark);
    }

    /// Emits the held last level. If any event of the book did not fit in the ring, the whole
    /// book is discarded instead of publishing a partial one.
    #[inline(always)]
    pub fn finish(mut self, out: &mut EventOut) {
        let e = match self.held.take() {
            Some(mut e) => {
                e.flags |= flags::LAST;
                e
            }
            None => {
                let mut e = self.base;
                e.flags = flags::EMPTY | flags::FIRST | flags::LAST;
                e
            }
        };
        if !self.complete || !out.emit(e) {
            out.rewind(self.mark);
        }
    }
}
