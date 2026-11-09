//! [CONTROL PLANE] Startup-time dispatch by `VenueKind`: exchange `match`, then the exchange
//! module dispatches to its market. Never called per message.

use crate::control_plane::configs_struct::{Environment, MarketDataType};
use crate::control_plane::startup::registry::VenueKind;
use crate::control_plane::runtime::supervisor::ConnectionContext;
use crate::venues::venue::{Credentials, Endpoint, RestEndpoint, VenueLimits};
use crate::venues::venue_pair::VenuePair;
use crate::venues::{binance, bitstamp};

pub fn limits(kind: VenueKind) -> VenueLimits {
    match kind {
        VenueKind::Binance(m) => binance::limits(m),
        VenueKind::Bitstamp(m) => bitstamp::limits(m),
    }
}

pub fn stream_name(kind: VenueKind, native_symbol: &str, dt: MarketDataType) -> Option<String> {
    match kind {
        VenueKind::Binance(m) => binance::stream_name(m, native_symbol, dt),
        VenueKind::Bitstamp(m) => bitstamp::stream_name(m, native_symbol, dt),
    }
}

pub fn route_key(kind: VenueKind, native_symbol: &str, dt: MarketDataType) -> String {
    match kind {
        VenueKind::Binance(m) => binance::route_key(m, native_symbol, dt),
        VenueKind::Bitstamp(m) => bitstamp::route_key(m, native_symbol, dt),
    }
}

pub fn endpoint(kind: VenueKind, env: Environment) -> Option<Endpoint> {
    match kind {
        VenueKind::Binance(m) => binance::endpoint(m, env),
        VenueKind::Bitstamp(m) => bitstamp::endpoint(m, env),
    }
}

pub fn refdata_endpoint(kind: VenueKind, env: Environment) -> Option<RestEndpoint> {
    match kind {
        VenueKind::Binance(m) => binance::refdata_endpoint(m, env),
        VenueKind::Bitstamp(m) => bitstamp::refdata_endpoint(m, env),
    }
}

pub fn parse_instruments(kind: VenueKind, body: &[u8]) -> anyhow::Result<Vec<VenuePair>> {
    match kind {
        VenueKind::Binance(m) => binance::parse_instruments(m, body),
        VenueKind::Bitstamp(m) => bitstamp::parse_instruments(m, body),
    }
}

/// Credentials are per exchange (shared by its markets).
pub fn load_credentials(kind: VenueKind) -> anyhow::Result<Credentials> {
    match kind {
        VenueKind::Binance(_) => binance::load_credentials(),
        VenueKind::Bitstamp(_) => Ok(Credentials::default()),
    }
}

/// Prepares one connection on the launch thread (decoder, session; errors fail startup) and
/// returns the connection thread's body, monomorphized per (exchange, market).
pub fn prepare_connection(kind: VenueKind, ctx: ConnectionContext) -> anyhow::Result<Box<dyn FnOnce() + Send>> {
    match kind {
        VenueKind::Binance(m) => binance::prepare(m, ctx),
        VenueKind::Bitstamp(m) => bitstamp::prepare(m, ctx),
    }
}
