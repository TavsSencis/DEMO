//! [CONTROL PLANE] Startup resolution: `NodeConfig` + `Registry` -> `StartupPlan`.
//! Pure (no I/O), deterministic and fully validated, so every configuration mistake fails
//! at startup instead of on the hot path.

use crate::control_plane::configs_struct::NodeConfig;
use crate::control_plane::configs_struct::DATA_TYPE_SLOTS;
use crate::control_plane::identity::{FeedId, VenueId};
use crate::control_plane::startup::registry::Registry;
use crate::control_plane::startup::resolver::error::PlanError;
use crate::control_plane::startup::resolver::plan::{
    ConnectionPlan, FeedSpec, MAX_ROUTE_KEY_LEN, MAX_ROUTE_KEYS_PER_CONNECTION, StartupPlan,
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

    // 1. Feeds, in config order. FeedId = position.
    let mut feeds: Vec<FeedSpec> = Vec::new();
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
        for &dt in &fc.subscriptions {
            if feeds.iter().any(|f| f.venue == fc.venue_id && f.instrument == fc.instrument_id && f.data_type.slot() == dt.slot()) {
                return Err(PlanError::DuplicateFeed(fc.venue_id, fc.instrument_id, dt));
            }
            let stream = venues::stream_name(venue.kind, &venue_instrument.native_symbol, dt)
                .ok_or(PlanError::Unsupported(venue.kind, dt))?;
            let route_key = venues::route_key(venue.kind, &venue_instrument.native_symbol, dt);
            if route_key.is_empty() || route_key.len() > MAX_ROUTE_KEY_LEN {
                return Err(PlanError::RouteKeyTooLong(route_key));
            }
            let id = u16::try_from(feeds.len()).map_err(|_| PlanError::TooManyFeeds)?;
            feeds.push(FeedSpec {
                id: FeedId(id),
                venue: fc.venue_id,
                instrument: fc.instrument_id,
                data_type: dt,
                stream,
                route_key,
                price_exp: venue_instrument.price_exp,
                qty_exp: venue_instrument.qty_exp,
            });
        }
    }
    if feeds.is_empty() {
        return Err(PlanError::NoFeeds);
    }

    // 2. One connection group per (venue, data-type class), in first-appearance order, so
    //    heavy depth traffic never queues in front of quotes/trades on the same socket.
    let mut groups: Vec<((VenueId, usize), Vec<usize>)> = Vec::new();
    for (i, f) in feeds.iter().enumerate() {
        let key = (f.venue, f.data_type.slot());
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
        let label = feeds[members[0]].data_type.label();
        let mut part = 0usize;
        let mut current: Option<ConnectionPlan> = None;

        for fi in members {
            let key = feeds[fi].route_key.clone();
            let needs_new = match &current {
                None => true,
                Some(c) => {
                    c.feeds.len() >= lim.max_streams_per_connection
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
                    feeds: Vec::new(),
                    route_keys: Vec::new(),
                    routes: Vec::new(),
                });
                part += 1;
            }
            let c = current.as_mut().expect("set above");
            let slot = match c.route_keys.iter().position(|k| *k == key) {
                Some(s) => s,
                None => {
                    c.route_keys.push(key);
                    c.routes.push([None; DATA_TYPE_SLOTS]);
                    c.route_keys.len() - 1
                }
            };
            c.routes[slot][feeds[fi].data_type.slot()] = Some(feeds[fi].id);
            c.feeds.push(feeds[fi].id);
        }
        if let Some(c) = current {
            connections.push(c);
        }
    }

    Ok(StartupPlan {
        environment: cfg.environment,
        runtime: cfg.runtime.clone(),
        feeds,
        connections,
    })
}
