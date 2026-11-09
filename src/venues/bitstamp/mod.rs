//! [CONTROL PLANE] Bitstamp: dispatch (`dispatch.rs`) to one module per market (`spot/`, later derivatives).
//! Public market data needs no credentials.

pub mod dispatch;
pub mod spot;

pub(crate) use dispatch::{endpoint, limits, parse_instruments, prepare, refdata_endpoint, route_key, stream_name};
