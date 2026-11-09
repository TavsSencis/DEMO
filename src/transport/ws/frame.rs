//! RFC 6455 frame header decoding (server -> client) and encoding (client -> server).
//! Pure functions, no I/O, no allocation.
#![deny(clippy::disallowed_methods, clippy::disallowed_macros)]

pub const OP_CONTINUATION: u8 = 0x0;
pub const OP_TEXT: u8 = 0x1;
pub const OP_BINARY: u8 = 0x2;
pub const OP_CLOSE: u8 = 0x8;
pub const OP_PING: u8 = 0x9;
pub const OP_PONG: u8 = 0xA;

pub const MAX_CONTROL_PAYLOAD: usize = 125;

/// Largest header a client frame can have: 2 + 8 (extended length) + 4 (mask).
pub const MAX_CLIENT_HEADER: usize = 14;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub fin: bool,
    pub opcode: u8,
    pub header_len: usize,
    pub payload_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameError {
    /// RSV1-3 set, but no extension was negotiated.
    ReservedBits,
    /// Server frames must not be masked (RFC 6455 §5.1).
    MaskedServerFrame,
    UnknownOpcode(u8),
    /// Control frame fragmented or longer than 125 bytes.
    BadControlFrame,
    /// Payload larger than the configured limit.
    TooLarge(u64),
    /// Continuation frame without a started message, or a new data frame
    /// while a fragmented message is still open.
    BadFragmentation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderParse {
    Ok(FrameHeader),
    Incomplete,
    Invalid(FrameError),
}

/// Parses a server frame header from the start of `buf`.
/// Never reads past `buf`; returns `Incomplete` when more bytes are needed.
#[inline(always)]
pub fn parse_header(buf: &[u8], max_payload: usize) -> HeaderParse {
    if buf.len() < 2 {
        return HeaderParse::Incomplete;
    }

    let b0 = buf[0];
    let b1 = buf[1];

    if b0 & 0x70 != 0 {
        return HeaderParse::Invalid(FrameError::ReservedBits);
    }
    if b1 & 0x80 != 0 {
        return HeaderParse::Invalid(FrameError::MaskedServerFrame);
    }

    let fin = b0 & 0x80 != 0;
    let opcode = b0 & 0x0F;
    let len7 = (b1 & 0x7F) as usize;

    let (header_len, payload_len) = if len7 < 126 {
        (2, len7 as u64)
    } else if len7 == 126 {
        if buf.len() < 4 {
            return HeaderParse::Incomplete;
        }
        (4, u16::from_be_bytes([buf[2], buf[3]]) as u64)
    } else {
        if buf.len() < 10 {
            return HeaderParse::Incomplete;
        }
        let mut b = [0u8; 8];
        b.copy_from_slice(&buf[2..10]);
        (10, u64::from_be_bytes(b))
    };

    match opcode {
        OP_CONTINUATION | OP_TEXT | OP_BINARY => {}
        OP_CLOSE | OP_PING | OP_PONG => {
            if !fin || payload_len > MAX_CONTROL_PAYLOAD as u64 {
                return HeaderParse::Invalid(FrameError::BadControlFrame);
            }
        }
        other => return HeaderParse::Invalid(FrameError::UnknownOpcode(other)),
    }

    if payload_len > max_payload as u64 {
        return HeaderParse::Invalid(FrameError::TooLarge(payload_len));
    }

    HeaderParse::Ok(FrameHeader {
        fin,
        opcode,
        header_len,
        payload_len: payload_len as usize,
    })
}

/// Writes one complete, masked client frame (FIN=1) into `out`.
/// Returns the number of bytes written, or `None` if `out` is too small.
#[inline]
pub fn encode_client_frame(
    out: &mut [u8],
    opcode: u8,
    payload: &[u8],
    mask: [u8; 4],
) -> Option<usize> {
    let len = payload.len();
    let ext: usize = if len < 126 {
        0
    } else if len <= u16::MAX as usize {
        2
    } else {
        8
    };
    let header_len = 2 + ext + 4;
    let total = header_len.checked_add(len)?;
    if total > out.len() {
        return None;
    }

    out[0] = 0x80 | (opcode & 0x0F);
    match ext {
        0 => out[1] = 0x80 | len as u8,
        2 => {
            out[1] = 0x80 | 126;
            out[2..4].copy_from_slice(&(len as u16).to_be_bytes());
        }
        _ => {
            out[1] = 0x80 | 127;
            out[2..10].copy_from_slice(&(len as u64).to_be_bytes());
        }
    }
    out[2 + ext..header_len].copy_from_slice(&mask);

    xor_mask(&mut out[header_len..total], payload, mask);
    Some(total)
}

/// `dst[i] = src[i] ^ mask[i % 4]`. Written as 8-byte chunks so the compiler vectorizes it.
#[inline(always)]
fn xor_mask(dst: &mut [u8], src: &[u8], mask: [u8; 4]) {
    debug_assert_eq!(dst.len(), src.len());
    // Repeating the 4 mask bytes in memory order keeps byte i aligned with mask[i % 4]
    // because every chunk starts at a multiple of 8.
    let m8 = [mask[0], mask[1], mask[2], mask[3], mask[0], mask[1], mask[2], mask[3]];
    let m64 = u64::from_ne_bytes(m8);

    let mut d = dst.chunks_exact_mut(8);
    let mut s = src.chunks_exact(8);
    for (dc, sc) in (&mut d).zip(&mut s) {
        let mut w = [0u8; 8];
        w.copy_from_slice(sc);
        dc.copy_from_slice(&(u64::from_ne_bytes(w) ^ m64).to_ne_bytes());
    }
    let base = src.len() - s.remainder().len();
    for (i, (db, sb)) in d.into_remainder().iter_mut().zip(s.remainder()).enumerate() {
        *db = sb ^ mask[(base + i) & 3];
    }
}
