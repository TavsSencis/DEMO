//! [CONTROL PLANE] StartupPlan types: immutable output of startup resolution, read by both planes.

use crate::control_plane::configs_struct::{DATA_TYPE_SLOTS, MarketDataType};
use crate::control_plane::configs_struct::Environment;
use crate::control_plane::configs_struct::RuntimeConfig;
use crate::control_plane::identity::{FeedId, InstrumentId, VenueId};
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
    /// Index == `FeedId`.
    pub feeds: Vec<FeedSpec>,
    pub connections: Vec<ConnectionPlan>,
}

#[derive(Debug, Clone)]
pub struct FeedSpec {
    pub id: FeedId,
    pub venue: VenueId,
    pub instrument: InstrumentId,
    pub data_type: MarketDataType,
    /// Venue channel name (e.g. "btcusdt@bestBidAsk", "live_trades_btcusd").
    pub stream: String,
    /// Bytes that identify this feed in inbound messages (symbol or channel, per venue).
    pub route_key: String,
    /// Fixed-point exponents for text venues (from the daily reference-data file).
    pub price_exp: i8,
    pub qty_exp: i8,
}

#[derive(Debug, Clone)]
pub struct ConnectionPlan {
    pub index: usize,
    /// Long name for logs, e.g. "BINANCE_SPOT/quote/0".
    pub name: String,
    /// Short thread name (Linux limit 15 bytes), e.g. "md0-quote".
    pub thread_name: String,
    pub venue_kind: VenueKind,
    /// Feeds carried by this connection.
    pub feeds: Vec<FeedId>,
    /// Route keys on this connection (symbols or channel names); index = route slot.
    pub route_keys: Vec<String>,
    /// `routes[route_slot][data_type_slot]` -> feed.
    pub routes: Vec<[Option<FeedId>; DATA_TYPE_SLOTS]>,
}

impl StartupPlan {
    #[inline]
    pub fn feed(&self, id: FeedId) -> &FeedSpec {
        &self.feeds[id.index()]
    }
}
