//! [CONTROL PLANE] `Registry`: the startup reference-data catalog. Canonical currencies and
//! instruments (derived from the node config's instrument keys), venues (code), plus the
//! venue instruments from the daily reference-data file. Built once at startup, read by the
//! resolver and `refdata`, then dropped.

use crate::control_plane::identity::{CurrencyId, InstrumentId, VenueId};
use crate::control_plane::startup::registry::currency::CurrencyDefinition;
use crate::control_plane::startup::registry::instrument::{InstrumentDefinition, InstrumentKey};
use crate::control_plane::startup::registry::venue::{self, VenueDefinition};
use crate::control_plane::startup::registry::venue_instrument::VenueInstrument;

#[derive(Debug, Clone)]
pub struct Registry {
    pub currencies: Vec<CurrencyDefinition>,
    pub instruments: Vec<InstrumentDefinition>,
    pub venues: Vec<VenueDefinition>,
    /// From the daily reference-data file; empty in `from_instruments`.
    pub venue_instruments: Vec<VenueInstrument>,
}

impl Registry {
    /// Canonical tables for `keys` (duplicates collapse): currencies deduplicated by code and
    /// instruments in first-appearance order, both with dense ids; venues from code.
    pub fn from_instruments(keys: &[InstrumentKey]) -> Self {
        let mut reg = Self {
            currencies: Vec::new(),
            instruments: Vec::new(),
            venues: venue::VENUES.to_vec(),
            venue_instruments: Vec::new(),
        };
        for key in keys {
            if reg.instrument_by_key(key).is_some() {
                continue;
            }
            let base = reg.currency_id(&key.base);
            let quote = reg.currency_id(&key.quote);
            let id = InstrumentId(reg.instruments.len() as u32);
            reg.instruments.push(InstrumentDefinition { id, kind: key.kind, base, quote });
        }
        reg
    }

    /// Existing currency id for `code`, or a new one.
    fn currency_id(&mut self, code: &str) -> CurrencyId {
        if let Some(c) = self.currencies.iter().find(|c| c.code == code) {
            return c.id;
        }
        let id = CurrencyId(self.currencies.len() as u16);
        self.currencies.push(CurrencyDefinition { id, code: code.to_string() });
        id
    }

    /// Adds the venue instruments loaded from the daily reference-data file.
    pub fn with_venue_instruments(mut self, venue_instruments: Vec<VenueInstrument>) -> Self {
        self.venue_instruments = venue_instruments;
        self
    }

    pub fn currency(&self, id: CurrencyId) -> Option<&CurrencyDefinition> {
        self.currencies.iter().find(|c| c.id == id)
    }

    pub fn instrument(&self, id: InstrumentId) -> Option<&InstrumentDefinition> {
        self.instruments.iter().find(|i| i.id == id)
    }

    pub fn venue(&self, id: VenueId) -> Option<&VenueDefinition> {
        self.venues.iter().find(|v| v.id == id)
    }

    pub fn venue_by_name(&self, name: &str) -> Option<&VenueDefinition> {
        self.venues.iter().find(|v| v.name == name)
    }

    pub fn instrument_by_key(&self, key: &InstrumentKey) -> Option<&InstrumentDefinition> {
        self.instruments.iter().find(|i| {
            i.kind == key.kind
                && self.currency(i.base).is_some_and(|c| c.code == key.base)
                && self.currency(i.quote).is_some_and(|c| c.code == key.quote)
        })
    }

    /// Canonical name of an instrument ("BTC/USDT").
    pub fn instrument_key(&self, id: InstrumentId) -> Option<InstrumentKey> {
        let i = self.instrument(id)?;
        Some(InstrumentKey {
            base: self.currency(i.base)?.code.clone(),
            quote: self.currency(i.quote)?.code.clone(),
            kind: i.kind,
        })
    }

    /// How `instrument` appears on `venue` (native symbol, exponents), if the venue lists it.
    pub fn venue_instrument(&self, venue: VenueId, instrument: InstrumentId) -> Option<&VenueInstrument> {
        self.venue_instruments.iter().find(|v| v.venue == venue && v.instrument == instrument)
    }
}
