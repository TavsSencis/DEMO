//! [CONTROL PLANE] Startup resolution: `NodeConfig` + `Registry` -> `StartupPlan`.
//! Pure (no I/O), deterministic and fully validated, so every configuration mistake fails
//! at startup instead of on the hot path.

use crate::control_plane::configs_struct::NodeConfig;
use crate::control_plane::configs_struct::DATA_TYPE_SLOTS;
use crate::control_plane::identity::{MarketId, StreamId, VenueId};
use crate::control_plane::startup::registry::Registry;
use crate::control_plane::startup::resolver::error::PlanError;
use crate::control_plane::startup::resolver::plan::{
    ConnectionPlan, MAX_ROUTE_KEY_LEN, MAX_ROUTE_KEYS_PER_CONNECTION, MarketSpec, RouteTarget, StartupPlan,
    StreamSpec,
};
use crate::venues;

pub fn resolve(cfg: &NodeConfig, reg: &Registry) -> Result<StartupPlan, PlanError> {
    reg.validate()?;
    let rt = &cfg.runtime;
    for (name, d) in [
        ("stale_timeout", rt.stale_timeout),
        ("reconnect.initial_backoff", rt.reconnect.initial_backoff),
        ("reconnect.connect_timeout", rt.reconnect.connect_timeout),
    ] {
        if d.is_zero() {
            return Err(PlanError::ZeroDuration(name));
        }
    }

    // 1. Markets (venue, instrument) in first-appearance order, streams in config order.
    //    MarketId and StreamId = position.
    let mut markets: Vec<MarketSpec> = Vec::new();
    let mut streams: Vec<StreamSpec> = Vec::new();
    for fc in &cfg.market_data.feeds {
        let venue = reg.venue(fc.venue_id).ok_or(PlanError::UnknownVenue(fc.venue_id))?;
        let venue_instrument = reg
            .venue_instrument(fc.venue_id, fc.instrument_id)
            .ok_or(PlanError::NotListed(fc.venue_id, fc.instrument_id))?;
        if !venue_instrument.trading {
            return Err(PlanError::NotTrading(fc.venue_id, fc.instrument_id));
        }
        if fc.subscriptions.is_empty() {
            return Err(PlanError::EmptySubscriptions(fc.venue_id, fc.instrument_id));
        }
        let market = match markets.iter().find(|m| m.venue == fc.venue_id && m.instrument == fc.instrument_id) {
            Some(m) => m.id,
            None => {
                let id = MarketId(u16::try_from(markets.len()).map_err(|_| PlanError::TooManyFeeds)?);
                markets.push(MarketSpec {
                    id,
                    venue: fc.venue_id,
                    instrument: fc.instrument_id,
                    price_exp: venue_instrument.price_exp,
                    qty_exp: venue_instrument.qty_exp,
                });
                id
            }
        };
        for &dt in &fc.subscriptions {
            if streams.iter().any(|s| s.market == market && s.data_type.slot() == dt.slot()) {
                return Err(PlanError::DuplicateFeed(fc.venue_id, fc.instrument_id, dt));
            }
            let stream = venues::stream_name(venue.kind, &venue_instrument.native_symbol, dt)
                .ok_or(PlanError::Unsupported(venue.kind, dt))?;
            let route_key = venues::route_key(venue.kind, &venue_instrument.native_symbol, dt);
            if route_key.is_empty() || route_key.len() > MAX_ROUTE_KEY_LEN {
                return Err(PlanError::RouteKeyTooLong(route_key));
            }
            let id = u16::try_from(streams.len()).map_err(|_| PlanError::TooManyFeeds)?;
            streams.push(StreamSpec { id: StreamId(id), market, data_type: dt, stream, route_key });
        }
    }
    if streams.is_empty() {
        return Err(PlanError::NoFeeds);
    }

    // 2. One connection group per (venue, data-type class), in first-appearance order, so
    //    heavy depth traffic never queues in front of quotes/trades on the same socket.
    let mut groups: Vec<((VenueId, usize), Vec<usize>)> = Vec::new();
    for (i, s) in streams.iter().enumerate() {
        let key = (markets[s.market.index()].venue, s.data_type.slot());
        match groups.iter_mut().find(|(k, _)| *k == key) {
            Some((_, members)) => members.push(i),
            None => groups.push((key, vec![i])),
        }
    }

    // 3. Split groups into connections within venue and route-table limits.
    let mut connections: Vec<ConnectionPlan> = Vec::new();
    for ((venue_id, _), members) in groups {
        let venue = reg.venue(venue_id).ok_or(PlanError::UnknownVenue(venue_id))?;
        let lim = venues::limits(venue.kind);
        let label = streams[members[0]].data_type.label();
        let mut part = 0usize;
        let mut current: Option<ConnectionPlan> = None;

        for si in members {
            let key = streams[si].route_key.clone();
            let needs_new = match &current {
                None => true,
                Some(c) => {
                    c.streams.len() >= lim.max_streams_per_connection
                        || (!c.route_keys.contains(&key) && c.route_keys.len() >= MAX_ROUTE_KEYS_PER_CONNECTION)
                }
            };
            if needs_new {
                if let Some(c) = current.take() {
                    connections.push(c);
                }
                let index = connections.len();
                current = Some(ConnectionPlan {
                    index,
                    name: format!("{}/{}/{}", venue.name, label, part),
                    thread_name: format!("md{index}-{label}"),
                    venue_kind: venue.kind,
                    streams: Vec::new(),
                    route_keys: Vec::new(),
                    routes: Vec::new(),
                });
                part += 1;
            }
            let c = current.as_mut().expect("set above");
            let s = &streams[si];
            let m = &markets[s.market.index()];
            let slot = match c.route_keys.iter().position(|k| *k == key) {
                Some(slot) => slot,
                None => {
                    c.route_keys.push(key);
                    c.routes.push(RouteTarget {
                        market: m.id,
                        instrument: m.instrument,
                        price_exp: m.price_exp,
                        qty_exp: m.qty_exp,
                        streams: [None; DATA_TYPE_SLOTS],
                    });
                    c.route_keys.len() - 1
                }
            };
            c.routes[slot].streams[s.data_type.slot()] = Some(s.id);
            c.streams.push(s.id);
        }
        if let Some(c) = current {
            connections.push(c);
        }
    }

    Ok(StartupPlan {
        environment: cfg.environment,
        runtime: cfg.runtime.clone(),
        streams,
        markets,
        connections,
    })
}
