//! [CONTROL PLANE] Binance Spot, SBE market-data streams (stream-sbe.binance.com).
//! Requests are JSON text frames; data is binary SBE (schema `spot_stream` id 1).

mod decoder;
mod refdata;
pub mod sbe;
pub mod venue;

pub use decoder::BinanceSpotHandler;
pub use venue::BinanceSpot;
