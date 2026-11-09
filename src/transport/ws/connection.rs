//! [CONTROL PLANE] An upgraded WebSocket connection, handed from connect to the session loop.

use std::net::TcpStream;

use openssl::ssl::SslStream;

/// An upgraded WebSocket connection plus any stream bytes that arrived with the upgrade response.
pub struct WsConnection {
    pub stream: SslStream<TcpStream>,
    pub pending: Vec<u8>,
}
