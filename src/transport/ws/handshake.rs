//! HTTP/1.1 -> WebSocket upgrade (RFC 6455 §4). Control plane: runs once per connection.

use std::io::{self, Read, Write};

use openssl::rand::rand_bytes;
use openssl::sha::sha1;

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const REQ_BUF_LEN: usize = 1024;
const RESP_BUF_LEN: usize = 4096;

/// Performs the upgrade with optional extra request headers (e.g. an API key).
/// Returns the bytes that arrived after the end of the HTTP response
/// (the start of the WebSocket stream); the reader must parse them before reading the socket.
pub fn upgrade<T: Read + Write>(
    stream: &mut T,
    host: &str,
    path: &str,
    headers: &[(&str, &str)],
) -> io::Result<Vec<u8>> {
    let mut key_raw = [0u8; 16];
    rand_bytes(&mut key_raw).map_err(io::Error::other)?;
    let mut key_b64 = [0u8; 24];
    b64_encode(&key_raw, &mut key_b64);

    let mut req = [0u8; REQ_BUF_LEN];
    let mut n = 0usize;
    let mut put = |part: &[u8]| -> io::Result<()> {
        let end = n + part.len();
        if end > req.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "upgrade request too long"));
        }
        req[n..end].copy_from_slice(part);
        n = end;
        Ok(())
    };
    for part in [
        b"GET ".as_slice(),
        path.as_bytes(),
        b" HTTP/1.1\r\nHost: ",
        host.as_bytes(),
        b"\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: ",
        &key_b64,
    ] {
        put(part)?;
    }
    for (name, value) in headers {
        put(b"\r\n")?;
        put(name.as_bytes())?;
        put(b": ")?;
        put(value.as_bytes())?;
    }
    put(b"\r\n\r\n")?;
    stream.write_all(&req[..n])?;

    let mut resp = [0u8; RESP_BUF_LEN];
    let mut len = 0usize;
    let header_end = loop {
        if len == resp.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "upgrade response header too big"));
        }
        let r = stream.read(&mut resp[len..])?;
        if r == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "closed during upgrade"));
        }
        // Rescan from 3 bytes back so a terminator split across reads is found.
        let scan_from = len.saturating_sub(3);
        len += r;
        if let Some(pos) = find(&resp[scan_from..len], b"\r\n\r\n") {
            break scan_from + pos + 4;
        }
    };
    let headers = &resp[..header_end];

    if !headers.starts_with(b"HTTP/1.1 101") {
        let status_end = find(headers, b"\r\n").unwrap_or(headers.len());
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("upgrade rejected: {}", String::from_utf8_lossy(&headers[..status_end])),
        ));
    }

    let mut concat = [0u8; 60];
    concat[..24].copy_from_slice(&key_b64);
    concat[24..].copy_from_slice(GUID);
    let mut accept = [0u8; 28];
    b64_encode(&sha1(&concat), &mut accept);

    if !has_header_value(headers, b"sec-websocket-accept", &accept) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Sec-WebSocket-Accept mismatch"));
    }

    Ok(resp[header_end..len].to_vec())
}

/// Standard base64 with padding. `dst.len()` must be `4 * ceil(src.len() / 3)`.
fn b64_encode(src: &[u8], dst: &mut [u8]) {
    debug_assert_eq!(dst.len(), src.len().div_ceil(3) * 4);
    for (chunk, out) in src.chunks(3).zip(dst.chunks_mut(4)) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        out[0] = B64[(b[0] >> 2) as usize];
        out[1] = B64[((b[0] & 0x03) << 4 | b[1] >> 4) as usize];
        out[2] = if chunk.len() > 1 { B64[((b[1] & 0x0F) << 2 | b[2] >> 6) as usize] } else { b'=' };
        out[3] = if chunk.len() > 2 { B64[(b[2] & 0x3F) as usize] } else { b'=' };
    }
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Case-insensitive header name match, exact value match (surrounding spaces/tabs trimmed).
fn has_header_value(headers: &[u8], name_lower: &[u8], value: &[u8]) -> bool {
    headers.split(|&b| b == b'\n').any(|line| {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let Some(colon) = line.iter().position(|&b| b == b':') else {
            return false;
        };
        let (name, rest) = line.split_at(colon);
        name.eq_ignore_ascii_case(name_lower) && rest[1..].trim_ascii() == value
    })
}
