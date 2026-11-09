//! WebSocket session loop for one connection thread. The thread owns the socket: it reads,
//! answers pings and writes control requests itself (no locks, no second writer).
//!
//! Receive buffer: one linear buffer allocated and prefaulted once per connection thread and
//! reused across sessions. Frames are parsed in place and handed to the handler as contiguous
//! slices (zero copy). Unread bytes (at most one partial frame) are moved to the front with
//! `copy_within` right away when small, otherwise when the tail runs low. Only fragmented messages (rare) are copied into a
//! preallocated reassembly buffer.
//!
//! Polling: non-blocking socket, the loop busy-polls (`WouldBlock` = idle, `spin_loop` hint)
//! and never sleeps or blocks. Timers (stale feed, shutdown, heartbeat, ack timeouts) run in
//! amortized housekeeping every `HOUSEKEEPING_EVERY` loop events, never per message.
#![deny(clippy::disallowed_methods, clippy::disallowed_macros)]

use std::io::{self, Read, Write};

use super::frame::{
    self, FrameError, HeaderParse, MAX_CLIENT_HEADER, OP_BINARY, OP_CLOSE, OP_CONTINUATION, OP_PING,
    OP_PONG, OP_TEXT,
};
use super::writer::{MaskGen, send_frame};
use crate::sys::clock::tsc;
use crate::sys::memory::prefault;

pub const RX_BUF_LEN: usize = 1 << 20;
pub const MAX_MESSAGE: usize = 256 * 1024;
const TX_BUF_LEN: usize = 64 * 1024;
const MIN_TAIL_SPACE: usize = 64 * 1024;
/// A leftover partial frame up to this size is moved to the buffer start right away, so the
/// working set stays in the first few KiB (hot in L1/L2, few TLB entries) instead of walking
/// through the whole buffer. Larger leftovers wait until the tail runs low.
const EAGER_COMPACT_MAX: usize = 4 * 1024;
/// Housekeeping runs once per this many loop events (reads or idle polls).
const HOUSEKEEPING_EVERY: u32 = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind {
    Text,
    Binary,
}

/// What the session loop calls into. Implemented by the connection core.
pub trait WsHandler {
    /// One complete message. `recv_tsc` = TSC when the read that completed it returned.
    /// Returns `true` when the venue asked for a reconnect (the session then ends).
    fn on_message(&mut self, kind: MessageKind, payload: &[u8], recv_tsc: u64) -> bool;
    /// Next text frame to send now (subscribe, heartbeat), if any. Called every loop
    /// iteration, so the empty case must be a couple of compares.
    fn next_outbound(&mut self) -> Option<&[u8]>;
    /// Amortized periodic work. Return `true` to stop the session (shutdown).
    fn housekeeping(&mut self) -> bool;
}

/// Why a session ended.
#[derive(Debug)]
pub enum ReaderExit {
    PeerClosedTcp,
    /// The venue asked the client to reconnect (e.g. server maintenance).
    VenueRequestedReconnect,
    CloseFrame { code: Option<u16> },
    /// No bytes for longer than the stale timeout.
    Stale,
    Shutdown,
    Io(io::Error),
    Protocol(FrameError),
}

#[derive(Debug, Clone, Copy)]
pub struct ReaderConfig {
    pub max_message: usize,
    /// 0 disables stale detection.
    pub stale_after_ticks: u64,
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self { max_message: MAX_MESSAGE, stale_after_ticks: 0 }
    }
}

/// Buffers owned by a connection thread for its whole life (all sessions).
pub struct ReaderBuffers {
    rx: Vec<u8>,
    tx: Vec<u8>,
    frag: Vec<u8>,
}

impl ReaderBuffers {
    #[allow(clippy::disallowed_methods, clippy::disallowed_macros)] // control plane: runs once at startup
    pub fn new(rx_len: usize, max_message: usize) -> Self {
        let mut rx = vec![0u8; rx_len];
        let mut tx = vec![0u8; TX_BUF_LEN];
        let mut frag = vec![0u8; max_message];
        prefault(&mut rx);
        prefault(&mut tx);
        prefault(&mut frag);
        frag.clear(); // keeps the (resident) capacity
        Self { rx, tx, frag }
    }
}

impl Default for ReaderBuffers {
    fn default() -> Self {
        Self::new(RX_BUF_LEN, MAX_MESSAGE)
    }
}

/// Runs one session until it ends. `pending` = bytes that arrived with the upgrade response.
pub fn run_reader<S, H>(
    stream: &mut S,
    pending: &[u8],
    mask: &mut MaskGen,
    bufs: &mut ReaderBuffers,
    cfg: &ReaderConfig,
    handler: &mut H,
) -> ReaderExit
where
    S: Read + Write,
    H: WsHandler,
{
    let ReaderBuffers { rx: buf, tx, frag } = bufs;
    let cap = buf.len();
    // A whole frame (header + payload) must always fit in the buffer.
    let max_payload = cfg.max_message.min(cap - MAX_CLIENT_HEADER).min(frag.capacity().max(1));
    let min_tail = MIN_TAIL_SPACE.min(cap / 4);

    frag.clear();
    let mut frag_kind: Option<MessageKind> = None;

    if pending.len() > cap {
        return ReaderExit::Protocol(FrameError::TooLarge(pending.len() as u64));
    }
    buf[..pending.len()].copy_from_slice(pending);
    let mut rd = 0usize;
    let mut wr = pending.len();
    let mut recv_tsc = tsc();
    let mut last_rx_tsc = recv_tsc;
    let mut since_housekeeping = 0u32;

    loop {
        // 1. Outbound control frames (rate limiting is the handler's job).
        while let Some(msg) = handler.next_outbound() {
            if let Err(e) = send_frame(stream, mask, tx, OP_TEXT, msg) {
                return ReaderExit::Io(e);
            }
        }

        // 2. Every complete frame in buf[rd..wr].
        loop {
            let h = match frame::parse_header(&buf[rd..wr], max_payload) {
                HeaderParse::Ok(h) => h,
                HeaderParse::Incomplete => break,
                HeaderParse::Invalid(e) => return ReaderExit::Protocol(e),
            };
            let total = h.header_len + h.payload_len;
            if wr - rd < total {
                break;
            }
            let payload = &buf[rd + h.header_len..rd + total];

            match h.opcode {
                OP_BINARY | OP_TEXT => {
                    if frag_kind.is_some() {
                        return ReaderExit::Protocol(FrameError::BadFragmentation);
                    }
                    let kind = if h.opcode == OP_BINARY { MessageKind::Binary } else { MessageKind::Text };
                    if h.fin {
                        if handler.on_message(kind, payload, recv_tsc) {
                            return ReaderExit::VenueRequestedReconnect;
                        }
                    } else {
                        frag.clear();
                        append(frag, payload);
                        frag_kind = Some(kind);
                    }
                }
                OP_CONTINUATION => {
                    let Some(kind) = frag_kind else {
                        return ReaderExit::Protocol(FrameError::BadFragmentation);
                    };
                    if frag.len() + payload.len() > max_payload {
                        return ReaderExit::Protocol(FrameError::TooLarge((frag.len() + payload.len()) as u64));
                    }
                    append(frag, payload);
                    if h.fin {
                        if handler.on_message(kind, frag, recv_tsc) {
                            return ReaderExit::VenueRequestedReconnect;
                        }
                        frag_kind = None;
                    }
                }
                OP_PING => {
                    if let Err(e) = send_frame(stream, mask, tx, OP_PONG, payload) {
                        return ReaderExit::Io(e);
                    }
                }
                OP_PONG => {}
                OP_CLOSE => {
                    let code = (payload.len() >= 2).then(|| u16::from_be_bytes([payload[0], payload[1]]));
                    return ReaderExit::CloseFrame { code };
                }
                _ => unreachable!("parse_header rejects other opcodes"),
            }
            rd += total;
        }

        // 3. Reclaim space. Unread bytes are at most one partial frame.
        if rd == wr {
            rd = 0;
            wr = 0;
        } else if rd > 0 && (wr - rd <= EAGER_COMPACT_MAX || cap - wr < min_tail) {
            buf.copy_within(rd..wr, 0);
            wr -= rd;
            rd = 0;
        }
        if wr == cap {
            return ReaderExit::Protocol(FrameError::TooLarge(cap as u64));
        }

        // 4. Read (or poll).
        match stream.read(&mut buf[wr..]) {
            Ok(0) => return ReaderExit::PeerClosedTcp,
            Ok(n) => {
                wr += n;
                recv_tsc = tsc();
                last_rx_tsc = recv_tsc;
                since_housekeeping += 1;
            }
            // Nothing yet: spin (no sleep, no blocking syscall).
            Err(e) if is_idle(&e) => {
                since_housekeeping += 1;
                core::hint::spin_loop();
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return ReaderExit::Io(e),
        }

        // 5. Amortized housekeeping.
        if since_housekeeping >= HOUSEKEEPING_EVERY {
            since_housekeeping = 0;
            if cfg.stale_after_ticks != 0 && tsc().wrapping_sub(last_rx_tsc) > cfg.stale_after_ticks {
                return ReaderExit::Stale;
            }
            if handler.housekeeping() {
                return ReaderExit::Shutdown;
            }
        }
    }
}

/// Appends a fragment inside the preallocated capacity. Never reallocates: `parse_header`
/// bounds every payload by `max_payload <= frag.capacity()`, and continuations are checked
/// against `max_payload` before appending.
#[inline(always)]
#[allow(clippy::disallowed_methods)] // bounded by capacity (see above): no allocation
fn append(frag: &mut Vec<u8>, payload: &[u8]) {
    debug_assert!(frag.len() + payload.len() <= frag.capacity());
    frag.extend_from_slice(payload);
}

/// Non-blocking "nothing yet" (`WouldBlock`; `TimedOut` kept for platforms that report it).
#[inline(always)]
fn is_idle(e: &io::Error) -> bool {
    matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut)
}
