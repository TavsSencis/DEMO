//! [DATA PLANE] What a venue decoder touches per message: the `VenueHandler` trait, the
//! `EmitCtx` it writes events through, and the `Control` signals (acks, reconnect) it raises
//! for the control plane to handle on a cold branch.
#![deny(clippy::disallowed_methods, clippy::disallowed_macros)]

use crate::control_plane::identity::StreamId;
use crate::data_plane::events::EventOut;
use crate::transport::ws::reader::MessageKind;

/// Venue response to a request, reported by the decoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ack {
    /// Venue confirms by channel (Bitstamp): this stream is subscribed.
    Subscribed(StreamId),
    /// Venue confirms by request id (Binance): every stream of that request.
    RequestOk(u64),
    RequestFailed(u64),
    /// Error that cannot be attributed to a request; pending subscriptions will time out.
    Error,
}

const MAX_ACKS: usize = 8;

/// Control signals a decoder raises while handling one message. Checked with a single
/// `is_set()` branch per message; handled on the cold path.
pub struct Control {
    acks: [Ack; MAX_ACKS],
    len: usize,
    pub reconnect: bool,
}

impl Default for Control {
    fn default() -> Self {
        Self { acks: [Ack::Error; MAX_ACKS], len: 0, reconnect: false }
    }
}

impl Control {
    #[inline(always)]
    pub fn is_set(&self) -> bool {
        self.len != 0 || self.reconnect
    }

    #[inline]
    pub fn ack(&mut self, a: Ack) {
        if self.len < MAX_ACKS {
            self.acks[self.len] = a;
            self.len += 1;
        }
    }

    /// Returns and clears the acks.
    pub fn take_acks(&mut self) -> ([Ack; MAX_ACKS], usize) {
        let n = self.len;
        self.len = 0;
        (self.acks, n)
    }
}

/// What a decoder may touch while handling one message.
pub struct EmitCtx<'a> {
    pub recv_tsc: u64,
    pub out: &'a mut EventOut,
    pub control: &'a mut Control,
}

pub trait VenueHandler: Send + 'static {
    /// Hot path: one complete WebSocket message (binary or text, as the venue uses them).
    fn on_message(&mut self, kind: MessageKind, payload: &[u8], ctx: &mut EmitCtx<'_>);
}
