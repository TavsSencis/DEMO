//! [CONTROL PLANE] Builds the daily reference data: reads every venue's instrument list,
//! keeps the pairs that are canonical Genesis instruments (base + quote currency + kind).

use std::time::Duration;

use anyhow::{Context, bail};

use crate::control_plane::configs_struct::Environment;
use crate::control_plane::startup::refdata::daily_file::{FORMAT_VERSION, RefDataEntry, RefDataFile, now_unix};
use crate::control_plane::startup::registry::{InstrumentKey, Registry, VenueInstrument};
use crate::venues;
use crate::transport::http;

/// Binance's full spot catalog is ~18 MB.
const MAX_BODY: usize = 64 * 1024 * 1024;
const HTTP_TIMEOUT: Duration = Duration::from_secs(20);

/// Reads every venue that has a reference-data endpoint for `env` and keeps the pairs that are
/// canonical instruments of `reg` (the instruments named in the node config).
pub fn build(env: Environment, reg: &Registry) -> anyhow::Result<RefDataFile> {
    reg.validate()?;
    let mut instruments: Vec<VenueInstrument> = Vec::new();

    for venue in &reg.venues {
        // A venue without an endpoint for this environment (e.g. no public sandbox) is skipped;
        // feeds configured for it fail later in the resolver with `NotListed`.
        let Some(ep) = venues::refdata_endpoint(venue.kind, env) else { continue };
        let body = http::get(ep.addr, ep.host, ep.path, HTTP_TIMEOUT, MAX_BODY).with_context(|| format!("{}: fetch", venue.name))?;
        let pairs = venues::parse_instruments(venue.kind, &body).with_context(|| format!("{}: parse", venue.name))?;

        for ins in &reg.instruments {
            let (Some(base), Some(quote)) = (reg.currency(ins.base), reg.currency(ins.quote)) else {
                continue;
            };
            let mut matches = pairs.iter().filter(|p| p.kind == ins.kind && p.base == base.code && p.quote == quote.code);
            let Some(p) = matches.next() else { continue };
            if matches.next().is_some() {
                bail!("{}: more than one pair for {}/{} {:?}", venue.name, base.code, quote.code, ins.kind);
            }
            instruments.push(VenueInstrument {
                venue: venue.id,
                instrument: ins.id,
                native_symbol: p.native_symbol.clone(),
                price_exp: p.price_exp,
                qty_exp: p.qty_exp,
                trading: p.trading,
            });
        }
    }

    reg.clone().with_venue_instruments(instruments.clone()).validate()?;
    let named = instruments
        .into_iter()
        .map(|vi| RefDataEntry {
            venue: reg.venue(vi.venue).map(|v| v.name.to_string()).unwrap_or_default(),
            instrument: reg.instrument_key(vi.instrument).as_ref().map(InstrumentKey::to_string).unwrap_or_default(),
            native_symbol: vi.native_symbol,
            price_exp: vi.price_exp,
            qty_exp: vi.qty_exp,
            trading: vi.trading,
        })
        .collect();
    Ok(RefDataFile { format_version: FORMAT_VERSION, generated_at_unix: now_unix(), environment: env, instruments: named })
}
