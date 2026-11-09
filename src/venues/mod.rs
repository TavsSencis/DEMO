//! [CONTROL PLANE] Venue protocol modules. `venue.rs` (trait Venue, control plane),
//! `venue_pair.rs` (reference-data pair), `dispatch.rs` (startup dispatch by kind),
//! `decode.rs` (VenueHandler / EmitCtx / acks, data plane). Each market directory has
//! `venue.rs` + `refdata.rs` [CONTROL PLANE] and `decoder.rs` [DATA PLANE].
//!
//! Everything venue-specific lives here: endpoints, channel names,
//! request formats, acknowledgements, keepalive, limits, credentials, connect sequence and the
//! hot-path decoder. The handler owns decoding; a separate subscription state tracks requests.
//! Resolved at startup and monomorphized per market.
//!
//! Layout: `venues/<exchange>/<market>/`, e.g. `binance/spot/`, `bitstamp/spot/`. Exchange
//! modules hold what their markets share (credentials) and dispatch to the markets.
//!
//! Adding a market: variant in the exchange's market enum + module + one arm per dispatch fn
//! in the exchange module. Adding an exchange: `VenueKind` variant + `venues/<exchange>/` +
//! one arm per fn in `dispatch.rs`. Nothing outside `venues/` and `registry` changes.

pub mod binance;
pub mod bitstamp;
pub mod decode;
pub mod dispatch;
pub mod venue;
pub mod venue_pair;

pub use decode::{Ack, Control, EmitCtx, VenueHandler};
pub use dispatch::{
    endpoint, limits, load_credentials, parse_instruments, refdata_endpoint, prepare_connection, route_key,
    stream_name,
};
pub use venue::{Credentials, Endpoint, Heartbeat, RestEndpoint, Venue, VenueLimits};
pub use venue_pair::{VenuePair, exp_from_increment};
