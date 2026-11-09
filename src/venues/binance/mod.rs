//! [CONTROL PLANE] Binance: shared credentials (`credentials.rs`) and dispatch (`dispatch.rs`) to one module per
//! market (`spot/`, later `usdm/`, `coinm/`, ...).

pub mod credentials;
pub mod dispatch;
pub mod spot;

pub use credentials::load_credentials;
pub(crate) use dispatch::{endpoint, limits, parse_instruments, prepare, refdata_endpoint, route_key, stream_name};
