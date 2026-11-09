//! [CONTROL PLANE] Every configuration type of a Genesis node (the schema). Values come from the
//! TOML file (`config/genesis.toml`) via `startup::config_file`.
//!
//! Sections: NodeConfig (root) -> Environment (which exchange endpoints) ->
//! MarketDataConfig (what to subscribe to) -> RuntimeConfig (how the process runs).

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::control_plane::identity::{InstrumentId, VenueId};

// ─── Node ────────────────────────────────────────────────────────────────────────────

/// Everything one Genesis node (process) needs to start.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Which exchange endpoints: real market or testnet.
    pub environment: Environment,
    /// What market data to subscribe to.
    pub market_data: MarketDataConfig,
    /// How the market-data runtime runs (staleness, recovery).
    pub runtime: RuntimeConfig,
    /// Where the start-of-day reference-data files are and how old one may be.
    pub refdata: RefDataConfig,
}

// ─── Reference data ──────────────────────────────────────────────────────────────────

/// Start-of-day reference data (venue symbols, precision, trading status).
/// `new_genesis refdata` writes a dated file into `dir`; the trading node loads the newest one
/// and refuses to start if it is older than `max_age`.
#[derive(Debug, Clone)]
pub struct RefDataConfig {
    pub dir: PathBuf,
    pub max_age: Duration,
}

// ─── Environment ─────────────────────────────────────────────────────────────────────

/// Which exchange endpoints the venues connect to. Independent of the runtime profile
/// (any profile can read live data or rehearse on a testnet).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Environment {
    /// Real exchange endpoints.
    Live,
    /// Exchange testnets (only venues that offer one; others fail at startup).
    Sandbox,
}

// ─── Market data ─────────────────────────────────────────────────────────────────────

/// Market-data requirements: WHAT Genesis needs, in venue-independent terms.
///
/// No protocols, native symbols, channel names, endpoints or messages here;
/// the resolver derives those from the registry and the venue modules.
#[derive(Debug, Clone, Default)]
pub struct MarketDataConfig {
    pub feeds: Vec<FeedConfig>,
}

#[derive(Debug, Clone)]
pub struct FeedConfig {
    pub venue_id: VenueId,
    pub instrument_id: InstrumentId,
    pub subscriptions: Vec<MarketDataType>,
}

impl FeedConfig {
    pub fn new(venue_id: VenueId, instrument_id: InstrumentId, subscriptions: &[MarketDataType]) -> Self {
        Self { venue_id, instrument_id, subscriptions: subscriptions.to_vec() }
    }
}

/// Universal market-data type, independent of venue, protocol or transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarketDataType {
    /// Best bid and best ask.
    Quote,
    /// Executed trades.
    Trade,
    /// Order-book snapshot with the given number of levels.
    OrderBookSnapshot { depth: u16 },
    /// Incremental order-book update.
    OrderBookDelta,
}

/// Number of `MarketDataType` classes. Used to size per-symbol route tables.
pub const DATA_TYPE_SLOTS: usize = 4;

impl MarketDataType {
    /// Dense index of the data-type class (depth is not part of the class).
    #[inline(always)]
    pub const fn slot(self) -> usize {
        match self {
            MarketDataType::Quote => 0,
            MarketDataType::Trade => 1,
            MarketDataType::OrderBookSnapshot { .. } => 2,
            MarketDataType::OrderBookDelta => 3,
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            MarketDataType::Quote => "quote",
            MarketDataType::Trade => "trade",
            MarketDataType::OrderBookSnapshot { .. } => "snap",
            MarketDataType::OrderBookDelta => "delta",
        }
    }
}

// ─── Runtime ─────────────────────────────────────────────────────────────────────────

/// HOW the market-data runtime runs: staleness and recovery. One unpinned thread per
/// connection that busy-polls its non-blocking socket (never sleeps); the production CPU
/// layout (pinning, locked memory, isolated cores) is designed with the production thread model.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// No bytes for this long -> connection is treated as dead and reconnected.
    /// Binance pings every ~20 s, so silence beyond that means the link is gone.
    pub stale_timeout: Duration,
    pub reconnect: ReconnectPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconnectPolicy {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    /// A session that lived at least this long resets the backoff.
    pub healthy_after: Duration,
    /// Max connection attempts per connection in a rolling 5-minute window
    /// (Binance allows 300 per IP; keep the sum over all connections well below).
    pub max_attempts_per_5min: u32,
    pub connect_timeout: Duration,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            healthy_after: Duration::from_secs(60),
            max_attempts_per_5min: 30,
            connect_timeout: Duration::from_secs(5),
        }
    }
}
