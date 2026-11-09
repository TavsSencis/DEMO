//! [CONTROL PLANE] Binance spot reference data: `GET /api/v3/exchangeInfo?permissions=SPOT`
//! (~18 MB, ~3700 symbols). Only `new_genesis refdata` uses this.

use anyhow::Context;
use serde::Deserialize;

use super::BinanceSpot;
use crate::control_plane::configs_struct::Environment;
use crate::control_plane::startup::registry::InstrumentKind;
use crate::venues::{RestEndpoint, Venue, VenuePair, exp_from_increment};

pub fn endpoint(env: Environment) -> Option<RestEndpoint> {
    let path = "/api/v3/exchangeInfo?permissions=SPOT";
    match env {
        Environment::Live => Some(RestEndpoint { addr: "api.binance.com:443", host: "api.binance.com", path }),
        Environment::Sandbox => {
            Some(RestEndpoint { addr: "testnet.binance.vision:443", host: "testnet.binance.vision", path })
        }
    }
}

#[derive(Deserialize)]
struct ExchangeInfo {
    symbols: Vec<Symbol>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Symbol {
    symbol: String,
    status: String,
    base_asset: String,
    quote_asset: String,
    #[serde(default)]
    is_spot_trading_allowed: bool,
    #[serde(default)]
    filters: Vec<Filter>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Filter {
    filter_type: String,
    #[serde(default)]
    tick_size: Option<String>,
    #[serde(default)]
    step_size: Option<String>,
}

pub fn parse(body: &[u8]) -> anyhow::Result<Vec<VenuePair>> {
    let info: ExchangeInfo = serde_json::from_slice(body).context("exchangeInfo JSON")?;
    let mut pairs = Vec::with_capacity(info.symbols.len());
    for s in info.symbols {
        if !s.is_spot_trading_allowed {
            continue;
        }
        // Exponents come from the tick size (PRICE_FILTER) and step size (LOT_SIZE).
        let tick = s.filters.iter().find(|f| f.filter_type == "PRICE_FILTER").and_then(|f| f.tick_size.as_deref());
        let step = s.filters.iter().find(|f| f.filter_type == "LOT_SIZE").and_then(|f| f.step_size.as_deref());
        let (Some(price_exp), Some(qty_exp)) = (tick.and_then(exp_from_increment), step.and_then(exp_from_increment))
        else {
            continue;
        };
        pairs.push(VenuePair {
            base: BinanceSpot::currency_code(&s.base_asset),
            quote: BinanceSpot::currency_code(&s.quote_asset),
            native_symbol: s.symbol,
            kind: InstrumentKind::Spot,
            price_exp,
            qty_exp,
            trading: s.status == "TRADING",
        });
    }
    Ok(pairs)
}
