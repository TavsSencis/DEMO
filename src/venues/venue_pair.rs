//! [CONTROL PLANE] A venue's tradable pair in Genesis vocabulary (start-of-day reference data),
//! plus helpers that turn venue precision into fixed-point exponents.

use crate::control_plane::startup::registry::InstrumentKind;

/// One tradable pair as a venue reports it, already normalized to Genesis vocabulary:
/// currency codes upper-case canonical ("BTC"), kind, exponents. Produced by
/// `Venue::parse_instruments`; mapped to canonical instruments by `control_plane::startup::refdata`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenuePair {
    pub native_symbol: String,
    pub base: String,
    pub quote: String,
    pub kind: InstrumentKind,
    pub price_exp: i8,
    pub qty_exp: i8,
    pub trading: bool,
}

/// Exponent of a decimal increment: "0.01000000" -> -2, "1.00" -> 0, "0.00001" -> -5.
pub fn exp_from_increment(increment: &str) -> Option<i8> {
    let s = increment.trim();
    let frac = match s.split_once('.') {
        Some((_, f)) => f.trim_end_matches('0'),
        None => "",
    };
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
        return None;
    }
    i8::try_from(frac.len()).ok().map(|n| -n)
}
