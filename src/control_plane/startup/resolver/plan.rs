//! [CONTROL PLANE] StartupPlan types: immutable output of startup resolution, read by both planes.

use crate::control_plane::configs_struct::{DATA_TYPE_SLOTS, MarketDataType};
use crate::control_plane::configs_struct::Environment;
use crate::control_plane::configs_struct::RuntimeConfig;
use crate::control_plane::identity::{InstrumentId, MarketId, StreamId, VenueId};
use crate::control_plane::startup::registry::VenueKind;

/// Route keys per connection (capacity of the hot-path route table).
pub const MAX_ROUTE_KEYS_PER_CONNECTION: usize = 64;
/// Longest route key (symbol or channel name) the hot-path route table accepts.
pub const MAX_ROUTE_KEY_LEN: usize = 32;

/// Immutable, fully validated result of startup resolution. Everything the runtime needs,
/// with every name already turned into a dense id.
#[derive(Debug, Clone)]
pub struct StartupPlan {
    pub environment: Environment,
    pub runtime: RuntimeConfig,
    /// Index == `StreamId`. One per (venue, instrument, data type) subscription.
    pub streams: Vec<StreamSpec>,
    /// Index == `MarketId`. One per (venue, instrument).
    pub markets: Vec<MarketSpec>,
    pub connections: Vec<ConnectionPlan>,
}

/// One subscribed venue stream (control plane: subscribe, ack, status).
#[derive(Debug, Clone)]
pub struct StreamSpec {
    pub id: StreamId,
    pub market: MarketId,
    pub data_type: MarketDataType,
    /// Venue channel name (e.g. "btcusdt@bestBidAsk", "live_trades_btcusd").
    pub stream: String,
    /// Bytes that identify this stream's messages (symbol or channel, per venue).
    pub route_key: String,
}

/// One canonical instrument on one venue: what every market-data event is tagged with.
#[derive(Debug, Clone)]
pub struct MarketSpec {
    pub id: MarketId,
    pub venue: VenueId,
    pub instrument: InstrumentId,
    /// Fixed-point exponents for text venues (from the daily reference-data file).
    pub price_exp: i8,
    pub qty_exp: i8,
}

/// What a route slot resolves to on the hot path, plus the streams behind it (cold path).
#[derive(Debug, Clone, Copy)]
pub struct RouteTarget {
    pub market: MarketId,
    pub instrument: InstrumentId,
    pub price_exp: i8,
    pub qty_exp: i8,
    /// `streams[data_type_slot]`: the subscriptions carried under this route key.
    pub streams: [Option<StreamId>; DATA_TYPE_SLOTS],
}

#[derive(Debug, Clone)]
pub struct ConnectionPlan {
    pub index: usize,
    /// Long name for logs, e.g. "BINANCE_SPOT/quote/0".
    pub name: String,
    /// Short thread name (Linux limit 15 bytes), e.g. "md0-quote".
    pub thread_name: String,
    pub venue_kind: VenueKind,
    /// Streams subscribed on this connection.
    pub streams: Vec<StreamId>,
    /// Route keys on this connection (symbols or channel names); index = route slot.
    pub route_keys: Vec<String>,
    /// `routes[route_slot]`: market, instrument and exponents for that key.
    pub routes: Vec<RouteTarget>,
}

impl StartupPlan {
    #[inline]
    pub fn stream(&self, id: StreamId) -> &StreamSpec {
        &self.streams[id.index()]
    }

    #[inline]
    pub fn market(&self, id: MarketId) -> &MarketSpec {
        &self.markets[id.index()]
    }
}
