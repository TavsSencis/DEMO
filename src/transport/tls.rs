use std::os::raw::{c_int, c_long};
use std::sync::OnceLock;

use std::io;
use std::net::TcpStream;
use std::time::Duration;

use openssl::ssl::{SslConnector, SslMethod, SslMode, SslStream};

/// From openssl/ssl.h (stable ABI values; the `openssl` crate has no `clear_mode` wrapper).
const SSL_CTRL_CLEAR_MODE: c_int = 78;

static TLS: OnceLock<SslConnector> = OnceLock::new();

/// Shared client TLS context, tuned for the hot read path:
/// - `RELEASE_BUFFERS` off: the `openssl` crate enables it by default, which makes OpenSSL
///   free its read buffer whenever it drains and malloc it again for the next record —
///   a heap allocation per TLS record on the hot path. Keep the buffers resident instead.
/// - read-ahead on: OpenSSL reads as many bytes as the socket has in one `recv`, instead of
///   one record header and then one record body per call.
pub fn tls() -> &'static SslConnector {
    TLS.get_or_init(|| {
        let mut b = SslConnector::builder(SslMethod::tls()).expect("OpenSSL client context");
        b.set_read_ahead(true);
        // SAFETY: `b.as_ptr()` is the live SSL_CTX owned by the builder; SSL_CTRL_CLEAR_MODE
        // only clears the given mode bits and takes no pointer argument.
        unsafe {
            openssl_sys::SSL_CTX_ctrl(
                b.as_ptr(),
                SSL_CTRL_CLEAR_MODE,
                SslMode::RELEASE_BUFFERS.bits() as c_long,
                std::ptr::null_mut(),
            );
        }
        // set_mode(empty) returns the current mode without changing it. Startup-only check.
        assert!(
            !b.set_mode(SslMode::empty()).contains(SslMode::RELEASE_BUFFERS),
            "OpenSSL kept SSL_MODE_RELEASE_BUFFERS enabled"
        );
        b.build()
    })
}

/// TLS client handshake over an established TCP stream (SNI + certificate/hostname checks).
pub fn handshake(host: &str, tcp_stream: TcpStream) -> io::Result<SslStream<TcpStream>> {
    let cfg = tls().configure().map_err(io::Error::other)?;
    cfg.connect(host, tcp_stream).map_err(io::Error::other)
}

/// TCP connect with timeout, then TLS handshake.
pub fn connect(addr: &str, host: &str, timeout: Duration) -> io::Result<SslStream<TcpStream>> {
    handshake(host, super::tcp::connect(addr, timeout)?)
}
