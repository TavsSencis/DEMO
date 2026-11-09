//! [CONTROL PLANE] Canonical instruments: base currency + quote currency + kind.
//! Named in config and in the daily reference-data file by `InstrumentKey` ("BTC/USDT");
//! numeric `InstrumentId`s are assigned at startup and exist only inside the process.
//! No venue data here: native symbols and precision come from the daily reference-data file.

use std::fmt;

use crate::control_plane::identity::{CurrencyId, InstrumentId};

/// Product class. Only `Spot` is wired to venues today; `Perpetual` marks venue pairs that
/// must never match a spot instrument (a venue's spot BTCUSDT vs perpetual BTCUSDT).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InstrumentKind {
    Spot,
    Perpetual,
}

/// Canonical Genesis instrument, independent of any venue.
#[derive(Debug, Clone, Copy)]
pub struct InstrumentDefinition {
    pub id: InstrumentId,
    pub kind: InstrumentKind,
    pub base: CurrencyId,
    pub quote: CurrencyId,
}

/// Canonical instrument name: `BASE/QUOTE` (spot), e.g. "BTC/USDT". The only way config and
/// the daily file refer to instruments.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InstrumentKey {
    pub base: String,
    pub quote: String,
    pub kind: InstrumentKind,
}

impl InstrumentKey {
    /// Parses "BTC/USDT" (spot). Codes are upper-cased; both must be non-empty alphanumeric.
    pub fn parse(s: &str) -> Option<Self> {
        let (base, quote) = s.trim().split_once('/')?;
        let ok = |c: &str| !c.is_empty() && c.bytes().all(|b| b.is_ascii_alphanumeric());
        if !ok(base) || !ok(quote) {
            return None;
        }
        Some(Self { base: base.to_ascii_uppercase(), quote: quote.to_ascii_uppercase(), kind: InstrumentKind::Spot })
    }
}

impl fmt::Display for InstrumentKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            InstrumentKind::Spot => write!(f, "{}/{}", self.base, self.quote),
            InstrumentKind::Perpetual => write!(f, "{}/{}-PERP", self.base, self.quote),
        }
    }
}
