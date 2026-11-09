//! Client -> server WebSocket frames: masking key generator and in-place frame writes on the
//! connection thread's own socket (pong, heartbeat, subscribe). No allocation per write.
#![deny(clippy::disallowed_methods, clippy::disallowed_macros)]

use std::io::{self, Write};
use std::time::{Duration, Instant};
use openssl::rand::rand_bytes;

use super::frame::encode_client_frame;

/// Client frame mask generator: xorshift64 seeded from OpenSSL's CSPRNG.
///
/// RFC 6455 §5.3 asks for unpredictable masks to protect HTTP intermediaries from cache
/// poisoning. Over TLS straight to the exchange there is no such intermediary, so a fast
/// PRNG seeded from strong entropy avoids a CSPRNG call per frame.
pub struct MaskGen(u64);

impl MaskGen {
    pub fn from_os_entropy() -> io::Result<Self> {
        let mut seed = [0u8; 8];
        rand_bytes(&mut seed).map_err(io::Error::other)?;
        // xorshift must never be seeded with 0.
        Ok(Self(u64::from_le_bytes(seed) | 1))
    }

    #[inline(always)]
    pub fn next_mask(&mut self) -> [u8; 4] {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        ((x >> 32) as u32).to_le_bytes()
    }
}

/// Encodes one masked client frame into `tx_buf` and writes it.
/// Fails (instead of silently dropping) when the payload does not fit `tx_buf`.
#[inline]
pub fn send_frame<W: Write>(
    stream: &mut W,
    mask: &mut MaskGen,
    tx_buf: &mut [u8],
    opcode: u8,
    payload: &[u8],
) -> io::Result<()> {
    // Frame larger than the tx buffer: a bug in a pre-built control frame. No allocation here.
    let n = encode_client_frame(tx_buf, opcode, payload, mask.next_mask())
        .ok_or(io::Error::from(io::ErrorKind::InvalidInput))?;
    write_all_retry(stream, &tx_buf[..n])
}

/// How long a non-blocking write may keep returning WouldBlock before giving up.
const WRITE_STALL_LIMIT: Duration = Duration::from_secs(2);

/// `write_all` that also works on non-blocking sockets: spins on WouldBlock (rare: the
/// socket send buffer is full) up to `WRITE_STALL_LIMIT`. The clock is read only then.
#[allow(clippy::disallowed_methods)] // Instant::now only after a write WouldBlock (cold stall path)
pub fn write_all_retry<W: Write>(stream: &mut W, mut buf: &[u8]) -> io::Result<()> {
    let mut deadline: Option<Instant> = None;
    while !buf.is_empty() {
        match stream.write(buf) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::WriteZero, "socket wrote 0 bytes")),
            Ok(n) => buf = &buf[n..],
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                let d = *deadline.get_or_insert_with(|| Instant::now() + WRITE_STALL_LIMIT);
                if Instant::now() > d {
                    return Err(io::Error::new(io::ErrorKind::TimedOut, "write stalled"));
                }
                core::hint::spin_loop();
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
