//! WebSocket (RFC 6455): `handshake` + `connection` (control plane: connect),
//! `frame`, `reader`, `writer` (data plane: the per-message loop).

pub mod connection;
pub mod frame;
pub mod handshake;
pub mod reader;
pub mod writer;

pub use connection::WsConnection;
