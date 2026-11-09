//! [CONTROL PLANE] Binance: startup dispatch from `BinanceMarket` to the market module.

use crate::control_plane::configs_struct::{Environment, MarketDataType};
use crate::control_plane::startup::registry::venue::BinanceMarket;
use crate::control_plane::runtime::supervisor::{self, ConnectionContext};
use crate::venues::binance::spot;
use crate::venues::{Endpoint, RestEndpoint, Venue, VenueLimits, VenuePair};

pub(crate) fn limits(m: BinanceMarket) -> VenueLimits {
    match m {
        BinanceMarket::Spot => spot::BinanceSpot::LIMITS,
    }
}

pub(crate) fn stream_name(m: BinanceMarket, native_symbol: &str, dt: MarketDataType) -> Option<String> {
    match m {
        BinanceMarket::Spot => spot::BinanceSpot::stream_name(native_symbol, dt),
    }
}

pub(crate) fn route_key(m: BinanceMarket, native_symbol: &str, dt: MarketDataType) -> String {
    match m {
        BinanceMarket::Spot => spot::BinanceSpot::route_key(native_symbol, dt),
    }
}

pub(crate) fn endpoint(m: BinanceMarket, env: Environment) -> Option<Endpoint> {
    match m {
        BinanceMarket::Spot => spot::BinanceSpot::endpoint(env),
    }
}

pub(crate) fn refdata_endpoint(m: BinanceMarket, env: Environment) -> Option<RestEndpoint> {
    match m {
        BinanceMarket::Spot => spot::BinanceSpot::refdata_endpoint(env),
    }
}

pub(crate) fn parse_instruments(m: BinanceMarket, body: &[u8]) -> anyhow::Result<Vec<VenuePair>> {
    match m {
        BinanceMarket::Spot => spot::BinanceSpot::parse_instruments(body),
    }
}

/// Prepares one connection (decoder, session) and returns its thread body, monomorphized for
/// the market's `Venue` type.
pub(crate) fn prepare(m: BinanceMarket, ctx: ConnectionContext) -> anyhow::Result<Box<dyn FnOnce() + Send>> {
    match m {
        BinanceMarket::Spot => Ok(Box::new(supervisor::prepare::<spot::BinanceSpot>(ctx)?)),
    }
}
