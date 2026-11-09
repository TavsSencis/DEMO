//! [CONTROL PLANE] `Node`: one running Genesis process, built from a `NodeConfig`
//!
//! `control_plane::configs_struct` is plain data (what to run, values from the TOML file via
//! `startup::config_file`); `Node` is what runs:
//!   Node::build(config, registry)  daily file -> registry -> StartupPlan -> connection threads
//!   node.market_data()    event rings (one per connection)
//!   node.shutdown()       stop every thread and wait for it
//! Order execution and risk will join the node in the next project phase.

use crate::control_plane::configs_struct::NodeConfig;

use crate::control_plane::startup::launch::{self, MarketData};
use crate::control_plane::startup::refdata::daily_file::load_latest;
use crate::control_plane::startup::registry::Registry;
use crate::control_plane::identity::{InstrumentId, VenueId};
use crate::control_plane::startup::resolver::{self, PlanError};

pub struct Node {
    market_data: MarketData,
}

impl Node {
    /// Loads the daily reference-data file, validates the config against the registry and
    /// starts the market-data threads. Every configuration or reference-data error is returned
    /// here, before any data flows. No network access before the WebSockets connect.
    pub fn build(config: NodeConfig, registry: Registry) -> anyhow::Result<Self> {
        let refdata = load_latest(&config.refdata.dir, config.environment, config.refdata.max_age)?;
        let venue_instruments = refdata.venue_instruments(&registry);
        let registry = registry.with_venue_instruments(venue_instruments);
        let plan = resolver::resolve(&config, &registry).map_err(|e| named(e, &registry))?;
        let market_data = launch::launch(plan)?;
        Ok(Self { market_data })
    }

    /// Event rings, one per connection.
    pub fn market_data(&mut self) -> &mut MarketData {
        &mut self.market_data
    }

    /// Stops every connection thread and waits for them.
    pub fn shutdown(self) {
        self.market_data.shutdown();
    }
}

/// Resolver errors that name a venue/instrument pair, rewritten with canonical names (the
/// numeric ids only mean something inside this process).
fn named(e: PlanError, reg: &Registry) -> anyhow::Error {
    let pair = |v: VenueId, i: InstrumentId| {
        let venue = reg.venue(v).map_or("?", |d| d.name);
        let instrument = reg.instrument_key(i).map_or_else(|| "?".to_string(), |k| k.to_string());
        format!("{venue} {instrument}")
    };
    match e {
        PlanError::NotListed(v, i) => {
            anyhow::anyhow!("{} is not in the daily reference data (run `new_genesis refdata`)", pair(v, i))
        }
        PlanError::NotTrading(v, i) => anyhow::anyhow!("{} is not trading on the venue today", pair(v, i)),
        PlanError::EmptySubscriptions(v, i) => anyhow::anyhow!("{}: feed has no data types", pair(v, i)),
        PlanError::DuplicateFeed(v, i, dt) => anyhow::anyhow!("{}: {dt:?} configured twice", pair(v, i)),
        other => anyhow::Error::new(other),
    }
}
