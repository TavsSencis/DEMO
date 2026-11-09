//! [CONTROL PLANE] Bitstamp spot reference data: `GET /api/v2/trading-pairs-info/`
//! (~250 pairs, spot only). Only `new_genesis refdata` uses this.

use anyhow::{Context, bail};
use serde::Deserialize;

use super::BitstampSpot;
use crate::control_plane::configs_struct::Environment;
use crate::control_plane::startup::registry::InstrumentKind;
use crate::venues::{RestEndpoint, Venue, VenuePair};

pub fn endpoint(env: Environment) -> Option<RestEndpoint> {
    match env {
        Environment::Live => Some(RestEndpoint {
            addr: "www.bitstamp.net:443",
            host: "www.bitstamp.net",
            path: "/api/v2/trading-pairs-info/",
        }),
        // No public sandbox.
        Environment::Sandbox => None,
    }
}

#[derive(Deserialize)]
struct Pair {
    /// "BTC/USD"
    name: String,
    /// "btcusd" — the symbol used in WebSocket channel names.
    url_symbol: String,
    base_decimals: u8,
    counter_decimals: u8,
    /// "Enabled" / "Disabled"
    trading: String,
}

pub fn parse(body: &[u8]) -> anyhow::Result<Vec<VenuePair>> {
    let raw: Vec<Pair> = serde_json::from_slice(body).context("trading-pairs-info JSON")?;
    let mut pairs = Vec::with_capacity(raw.len());
    for p in raw {
        let Some((base, quote)) = p.name.split_once('/') else {
            bail!("unexpected pair name {:?}", p.name);
        };
        // The endpoint lists spot pairs; guard against derivatives appearing later.
        let kind = if p.name.to_ascii_uppercase().contains("PERP") || p.url_symbol.contains("perp") {
            InstrumentKind::Perpetual
        } else {
            InstrumentKind::Spot
        };
        pairs.push(VenuePair {
            base: BitstampSpot::currency_code(base),
            quote: BitstampSpot::currency_code(quote),
            native_symbol: p.url_symbol,
            kind,
            price_exp: -(p.counter_decimals as i8),
            qty_exp: -(p.base_decimals as i8),
            trading: p.trading == "Enabled",
        });
    }
    Ok(pairs)
}
