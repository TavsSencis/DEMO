use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

/// Connects with a timeout (DNS resolution happens here, control plane only).
/// Read/write timeouts are set for the handshake; the caller switches the socket to its
/// runtime mode (non-blocking busy-poll) afterwards.
pub fn connect(addr: &str, timeout: Duration) -> io::Result<TcpStream> {
    let mut last_err = None;
    for sa in addr.to_socket_addrs()? {
        match TcpStream::connect_timeout(&sa, timeout) {
            Ok(s) => {
                s.set_nodelay(true)?;
                s.set_read_timeout(Some(timeout))?;
                s.set_write_timeout(Some(timeout))?;
                return Ok(s);
            }
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("{addr}: no address"))))
}
