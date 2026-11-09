//! [CONTROL PLANE] Bitstamp: startup dispatch from `BitstampMarket` to the market module.

use crate::control_plane::configs_struct::{Environment, MarketDataType};
use crate::control_plane::startup::registry::venue::BitstampMarket;
use crate::control_plane::runtime::supervisor::{self, ConnectionContext};
use crate::venues::bitstamp::spot;
use crate::venues::{Endpoint, RestEndpoint, Venue, VenueLimits, VenuePair};

pub(crate) fn limits(m: BitstampMarket) -> VenueLimits {
    match m {
        BitstampMarket::Spot => spot::BitstampSpot::LIMITS,
    }
}

pub(crate) fn stream_name(m: BitstampMarket, native_symbol: &str, dt: MarketDataType) -> Option<String> {
    match m {
        BitstampMarket::Spot => spot::BitstampSpot::stream_name(native_symbol, dt),
    }
}

pub(crate) fn route_key(m: BitstampMarket, native_symbol: &str, dt: MarketDataType) -> String {
    match m {
        BitstampMarket::Spot => spot::BitstampSpot::route_key(native_symbol, dt),
    }
}

pub(crate) fn endpoint(m: BitstampMarket, env: Environment) -> Option<Endpoint> {
    match m {
        BitstampMarket::Spot => spot::BitstampSpot::endpoint(env),
    }
}

pub(crate) fn refdata_endpoint(m: BitstampMarket, env: Environment) -> Option<RestEndpoint> {
    match m {
        BitstampMarket::Spot => spot::BitstampSpot::refdata_endpoint(env),
    }
}

pub(crate) fn parse_instruments(m: BitstampMarket, body: &[u8]) -> anyhow::Result<Vec<VenuePair>> {
    match m {
        BitstampMarket::Spot => spot::BitstampSpot::parse_instruments(body),
    }
}

/// Prepares one connection (decoder, session) and returns its thread body, monomorphized for
/// the market's `Venue` type.
pub(crate) fn prepare(m: BitstampMarket, ctx: ConnectionContext) -> anyhow::Result<Box<dyn FnOnce() + Send>> {
    match m {
        BitstampMarket::Spot => Ok(Box::new(supervisor::prepare::<spot::BitstampSpot>(ctx)?)),
    }
}
