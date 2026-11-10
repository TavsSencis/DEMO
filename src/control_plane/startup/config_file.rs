//! [CONTROL PLANE] The node config file (TOML, default `config/genesis.toml`): the one place
//! where an operator states what to run. Loaded once at startup into the typed `NodeConfig`
//! (schema in `configs_struct.rs`) plus the canonical `Registry` derived from the feeds'
//! instruments. Unknown keys are rejected; every name is checked here, before any network.
//!
//! ```toml
//! environment = "Live"                  # Live | Sandbox
//! [refdata]
//! dir = "/var/lib/new_genesis/refdata"
//! max_age_hours = 48
//! [runtime]
//! stale_timeout_ms = 30000              # [runtime.reconnect] optional (defaults)
//! [[feed]]
//! venue = "BINANCE_SPOT"                # a venue name from registry/venue.rs
//! instrument = "BTC/USDT"               # canonical BASE/QUOTE (spot)
//! data = ["quote", "trade", "delta"]    # quote | trade | delta | snapshotN (e.g. snapshot20)
//! ```

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, bail};
use serde::Deserialize;

use crate::control_plane::configs_struct::{
    Environment, FeedConfig, MarketDataConfig, MarketDataType, NodeConfig, ReconnectPolicy, RefDataConfig,
    RuntimeConfig,
};
use crate::control_plane::startup::registry::{InstrumentKey, Registry};

/// Default path, relative to the working directory; `GENESIS_CONFIG` overrides it.
pub const DEFAULT_PATH: &str = "config/genesis.toml";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    environment: Environment,
    refdata: RefDataSection,
    runtime: RuntimeSection,
    feed: Vec<FeedSection>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RefDataSection {
    dir: PathBuf,
    max_age_hours: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeSection {
    stale_timeout_ms: u64,
    #[serde(default)]
    reconnect: Option<ReconnectSection>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReconnectSection {
    initial_backoff_ms: u64,
    max_backoff_ms: u64,
    healthy_after_s: u64,
    max_attempts_per_5min: u32,
    connect_timeout_ms: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FeedSection {
    venue: String,
    instrument: String,
    data: Vec<String>,
}

/// Config file path: `GENESIS_CONFIG` or `DEFAULT_PATH`.
pub fn path() -> PathBuf {
    std::env::var_os("GENESIS_CONFIG").map_or_else(|| PathBuf::from(DEFAULT_PATH), PathBuf::from)
}

/// Reads and checks the file. Returns the node config (feeds as dense ids, in file order) and
/// the canonical registry those ids refer to.
pub fn load(path: &Path) -> anyhow::Result<(NodeConfig, Registry)> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
    let file: File = toml::from_str(&text).with_context(|| format!("parse config {}", path.display()))?;

    // Canonical instruments = the ones the feeds name, in first-appearance order.
    let mut keys = Vec::with_capacity(file.feed.len());
    for (i, f) in file.feed.iter().enumerate() {
        let key = InstrumentKey::parse(&f.instrument)
            .with_context(|| format!("feed {i}: bad instrument {:?} (expected BASE/QUOTE, e.g. \"BTC/USDT\")", f.instrument))?;
        keys.push(key);
    }
    let registry = Registry::from_instruments(&keys);

    let mut feeds = Vec::with_capacity(file.feed.len());
    for (i, (f, key)) in file.feed.iter().zip(&keys).enumerate() {
        let Some(venue) = registry.venue_by_name(&f.venue) else {
            let known: Vec<&str> = registry.venues.iter().map(|v| v.name).collect();
            bail!("feed {i}: unknown venue {:?} (known: {known:?})", f.venue);
        };
        let instrument = registry.instrument_by_key(key).context("instrument registered above")?;
        let data = f
            .data
            .iter()
            .map(|d| data_type(d).with_context(|| format!("feed {i}: bad data type {d:?} (quote | trade | delta | snapshotN)")))
            .collect::<anyhow::Result<Vec<_>>>()?;
        feeds.push(FeedConfig::new(venue.id, instrument.id, &data));
    }

    let reconnect = file.runtime.reconnect.map_or_else(ReconnectPolicy::default, |r| ReconnectPolicy {
        initial_backoff: Duration::from_millis(r.initial_backoff_ms),
        max_backoff: Duration::from_millis(r.max_backoff_ms),
        healthy_after: Duration::from_secs(r.healthy_after_s),
        max_attempts_per_5min: r.max_attempts_per_5min,
        connect_timeout: Duration::from_millis(r.connect_timeout_ms),
    });
    let config = NodeConfig {
        environment: file.environment,
        market_data: MarketDataConfig { feeds },
        runtime: RuntimeConfig { stale_timeout: Duration::from_millis(file.runtime.stale_timeout_ms), reconnect },
        refdata: RefDataConfig {
            dir: file.refdata.dir,
            max_age: Duration::from_secs(file.refdata.max_age_hours * 3600),
        },
    };
    Ok((config, registry))
}

/// "quote" | "trade" | "delta" | "snapshotN" (N = depth, e.g. "snapshot20").
fn data_type(s: &str) -> Option<MarketDataType> {
    match s {
        "quote" => Some(MarketDataType::Quote),
        "trade" => Some(MarketDataType::Trade),
        "delta" => Some(MarketDataType::OrderBookDelta),
        _ => {
            let depth: u16 = s.strip_prefix("snapshot")?.parse().ok()?;
            (depth > 0).then_some(MarketDataType::OrderBookSnapshot { depth })
        }
    }
}
