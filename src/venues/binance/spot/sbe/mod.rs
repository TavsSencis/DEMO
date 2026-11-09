//! [DATA PLANE] Binance spot SBE decoders (schema `spot_stream`, id 1).
#![deny(clippy::disallowed_methods, clippy::disallowed_macros)]

pub mod best_bidask;
pub mod depth_20;
pub mod depth_diff;
pub mod dispatch;
pub mod codec;
pub mod trade;
