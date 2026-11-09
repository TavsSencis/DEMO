//! Venue-agnostic network transport: TCP, TLS, WebSocket and a minimal HTTPS GET. Knows nothing
//! about exchanges; `venues/` decides what to send, the supervisor and session decide when.
//! A kernel-bypass backend would live here next to the kernel TCP one.
//! Planes per file: `tcp.rs`, `tls.rs`, `http.rs`, `ws/handshake.rs` = control plane;
//! `ws/frame.rs`, `ws/reader.rs`, `ws/writer.rs` = data plane.

pub mod http;
pub mod tcp;
pub mod tls;
pub mod ws;
