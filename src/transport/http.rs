//! [CONTROL PLANE] Minimal blocking HTTPS GET (used by start-of-day reference data).
//! HTTP/1.1 over the existing OpenSSL TLS connection (`transport::tls`), `Connection: close`,
//! identity encoding, `Content-Length` or `chunked` bodies, bounded size. Never used on the
//! hot path.

use std::io::{Read, Write};
use std::time::Duration;

use anyhow::{Context, bail};

use crate::transport::tls;

const READ_CHUNK: usize = 64 * 1024;
const MAX_HEADER: usize = 64 * 1024;

/// Returns the body of a `200 OK` response for `GET path` on `host` (`addr` = host:port).
pub fn get(addr: &str, host: &str, path: &str, timeout: Duration, max_body: usize) -> anyhow::Result<Vec<u8>> {
    let mut stream = tls::connect(addr, host, timeout).with_context(|| format!("connect {host}"))?;
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nUser-Agent: new_genesis-refdata\r\nAccept: application/json\r\n\
         Accept-Encoding: identity\r\nConnection: close\r\n\r\n",
        path, host
    );
    stream.write_all(request.as_bytes()).context("send request")?;

    let mut raw: Vec<u8> = Vec::with_capacity(READ_CHUNK);
    let mut chunk = vec![0u8; READ_CHUNK];
    loop {
        if let Some(body) = complete_body(&raw, max_body)? {
            return Ok(body);
        }
        if raw.len() > MAX_HEADER + max_body + 64 * 1024 {
            bail!("{host}{path}: response larger than {max_body} bytes");
        }
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => raw.extend_from_slice(&chunk[..n]),
            // Some servers close TLS without close_notify once the body is sent.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e).context("read response"),
        }
    }
    // Peer closed: the body may be delimited by EOF (no Content-Length, not chunked).
    match complete_body(&raw, max_body)? {
        Some(body) => Ok(body),
        None => eof_body(&raw),
    }
}

struct Head {
    status: u16,
    body_start: usize,
    content_length: Option<usize>,
    chunked: bool,
}

fn parse_head(raw: &[u8]) -> anyhow::Result<Option<Head>> {
    let Some(end) = raw.windows(4).position(|w| w == b"\r\n\r\n") else {
        if raw.len() > MAX_HEADER {
            bail!("response header too large");
        }
        return Ok(None);
    };
    let head = std::str::from_utf8(&raw[..end]).context("non-UTF-8 response header")?;
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .with_context(|| format!("bad status line: {status_line}"))?;
    let (mut content_length, mut chunked) = (None, false);
    for line in lines {
        let Some((name, value)) = line.split_once(':') else { continue };
        let value = value.trim();
        match name.trim().to_ascii_lowercase().as_str() {
            "content-length" => content_length = Some(value.parse().context("bad Content-Length")?),
            "transfer-encoding" => chunked = value.to_ascii_lowercase().contains("chunked"),
            _ => {}
        }
    }
    Ok(Some(Head { status, body_start: end + 4, content_length, chunked }))
}

/// `Some(body)` once the response is complete according to its framing.
fn complete_body(raw: &[u8], max_body: usize) -> anyhow::Result<Option<Vec<u8>>> {
    let Some(h) = parse_head(raw)? else { return Ok(None) };
    if h.status != 200 {
        let preview = String::from_utf8_lossy(&raw[h.body_start..raw.len().min(h.body_start + 300)]).into_owned();
        bail!("HTTP {}: {preview}", h.status);
    }
    let body = &raw[h.body_start..];
    if h.chunked {
        return decode_chunked(body, max_body);
    }
    if let Some(len) = h.content_length {
        if len > max_body {
            bail!("Content-Length {len} exceeds limit {max_body}");
        }
        return Ok((body.len() >= len).then(|| body[..len].to_vec()));
    }
    Ok(None)
}

fn eof_body(raw: &[u8]) -> anyhow::Result<Vec<u8>> {
    let h = parse_head(raw)?.context("connection closed before the response header")?;
    if h.chunked || h.content_length.is_some() {
        bail!("connection closed before the full body arrived");
    }
    Ok(raw[h.body_start..].to_vec())
}

/// Decodes a chunked body; `None` while incomplete.
fn decode_chunked(mut b: &[u8], max_body: usize) -> anyhow::Result<Option<Vec<u8>>> {
    let mut out = Vec::new();
    loop {
        let Some(eol) = b.windows(2).position(|w| w == b"\r\n") else { return Ok(None) };
        let size_line = std::str::from_utf8(&b[..eol]).context("bad chunk size line")?;
        let size_hex = size_line.split(';').next().unwrap_or_default().trim();
        let size = usize::from_str_radix(size_hex, 16).with_context(|| format!("bad chunk size {size_hex:?}"))?;
        let data_start = eol + 2;
        if size == 0 {
            return Ok(Some(out));
        }
        if b.len() < data_start + size + 2 {
            return Ok(None);
        }
        out.extend_from_slice(&b[data_start..data_start + size]);
        if out.len() > max_body {
            bail!("chunked body exceeds limit {max_body}");
        }
        b = &b[data_start + size + 2..];
    }
}
