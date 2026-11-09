//! [DATA PLANE] Per-message work of one connection, monomorphized per venue.
//!
//! `Session` is what the transport's hot loop (`transport::ws::reader::run_reader`) calls into:
//! `on_message` for every WebSocket message, `next_outbound` every loop iteration,
//! `housekeeping` amortized. Calls into the control plane (`Subscriptions`)
//! happen only on `#[cold]` branches or amortized housekeeping, never per message.
//! Built and driven by `control_plane::runtime::supervisor`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

use crate::control_plane::runtime::subscriptions::Subscriptions;
use crate::data_plane::events::EventOut;
use crate::sys::clock::tsc;
use crate::transport::ws::reader::{MessageKind, WsHandler};
use crate::venues::{Control, EmitCtx, Venue, VenueHandler};

pub struct Session<V: Venue> {
    handler: V::Handler,
    subs: Subscriptions,
    out: EventOut,
    shutdown: Arc<AtomicBool>,
    control: Control,
    /// Client heartbeat (venues that need one): period in TSC ticks, next due time, due flag.
    heartbeat_ticks: u64,
    next_heartbeat_tsc: u64,
    heartbeat_due: bool,
}

impl<V: Venue> Session<V> {
    /// Control plane: built once per connection thread, reused across reconnects.
    pub fn new(
        handler: V::Handler,
        subs: Subscriptions,
        out: EventOut,
        shutdown: Arc<AtomicBool>,
        heartbeat_ticks: u64,
    ) -> Self {
        Self {
            handler,
            subs,
            out,
            shutdown,
            control: Control::default(),
            heartbeat_ticks,
            next_heartbeat_tsc: 0,
            heartbeat_due: false,
        }
    }

    /// New session: queue the subscription replay and restart the heartbeat timer. Cold.
    #[cold]
    pub fn on_session_start(&mut self) {
        self.subs.on_connected();
        self.control = Control::default();
        self.heartbeat_due = false;
        self.next_heartbeat_tsc = tsc().wrapping_add(self.heartbeat_ticks);
    }

    /// Session ended: confirmed feeds go Down and the events are published. Cold.
    #[cold]
    pub fn on_session_end(&mut self) {
        self.subs.on_disconnected(&mut self.out, tsc());
        self.out.flush();
    }

    /// Applies acks and returns whether the venue asked for a reconnect. Cold.
    #[cold]
    #[inline(never)]
    fn handle_control(&mut self) -> bool {
        let reconnect = std::mem::take(&mut self.control.reconnect);
        let (acks, n) = self.control.take_acks();
        let now = tsc();
        for ack in &acks[..n] {
            self.subs.on_ack(*ack, &mut self.out, now);
        }
        reconnect
    }
}

impl<V: Venue> WsHandler for Session<V> {
    /// HOT: every WebSocket message.
    #[inline(always)]
    fn on_message(&mut self, kind: MessageKind, payload: &[u8], recv_tsc: u64) -> bool {
        let mut ctx = EmitCtx {
            recv_tsc,
            active: &self.subs.active,
            out: &mut self.out,
            control: &mut self.control,
        };
        self.handler.on_message(kind, payload, &mut ctx);
        let reconnect = if self.control.is_set() { self.handle_control() } else { false };
        // One Release store per message, however many events it produced.
        self.out.flush();
        reconnect
    }

    /// HOT (empty case): every loop iteration. Sending is rare and cold.
    #[inline(always)]
    fn next_outbound(&mut self) -> Option<&[u8]> {
        if self.heartbeat_due {
            self.heartbeat_due = false;
            if let Some(hb) = V::HEARTBEAT {
                return Some(hb.payload);
            }
        }
        if !self.subs.has_outbound() {
            return None;
        }
        self.subs.next_outbound()
    }

    /// Amortized (every 1024 loop events): heartbeat timer, ack timeouts, subscribe retries,
    /// shutdown check.
    fn housekeeping(&mut self) -> bool {
        let now = tsc();
        if self.heartbeat_ticks != 0 && now.wrapping_sub(self.next_heartbeat_tsc) < u64::MAX / 2 {
            self.heartbeat_due = true;
            self.next_heartbeat_tsc = now.wrapping_add(self.heartbeat_ticks);
        }
        if self.subs.needs_timer() {
            self.subs.on_timer(&mut self.out, now);
            self.out.flush();
        }
        self.shutdown.load(Relaxed)
    }
}
