//! [CONTROL PLANE] Bitstamp spot, WebSocket API v2 (wss://ws.bitstamp.net), JSON text frames.
//! One channel per subscribe request; confirmations name the channel; the server may ask
//! the client to reconnect (`bts:request_reconnect`); clients may send `bts:heartbeat`.

mod decoder;
mod refdata;
pub mod venue;

pub use decoder::BitstampSpotHandler;
pub use venue::BitstampSpot;
