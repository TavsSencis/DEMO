//! [CONTROL PLANE] Integrity checks of the `Registry` before anything is resolved from it.

use crate::control_plane::startup::registry::catalog::Registry;
use crate::control_plane::startup::registry::error::RegistryError;

impl Registry {
    /// Duplicate ids/codes, instruments with unknown currencies, venue instruments that refer
    /// to unknown venues or instruments, and two pairs or two native symbols for the same thing
    /// on one venue are rejected.
    pub fn validate(&self) -> Result<(), RegistryError> {
        for (i, c) in self.currencies.iter().enumerate() {
            if self.currencies[..i].iter().any(|o| o.id == c.id || o.code == c.code) {
                return Err(RegistryError::DuplicateCurrency(c.id));
            }
        }
        for (i, ins) in self.instruments.iter().enumerate() {
            if self.instruments[..i].iter().any(|o| o.id == ins.id) {
                return Err(RegistryError::DuplicateInstrument(ins.id));
            }
            if self.currency(ins.base).is_none() || self.currency(ins.quote).is_none() {
                return Err(RegistryError::UnknownCurrency(ins.id));
            }
        }
        for (i, v) in self.venues.iter().enumerate() {
            if self.venues[..i].iter().any(|o| o.id == v.id) {
                return Err(RegistryError::DuplicateVenue(v.id));
            }
        }
        for (i, vi) in self.venue_instruments.iter().enumerate() {
            if self.venue(vi.venue).is_none() {
                return Err(RegistryError::VenueInstrumentUnknownVenue(vi.venue));
            }
            if self.instrument(vi.instrument).is_none() {
                return Err(RegistryError::VenueInstrumentUnknownInstrument(vi.instrument));
            }
            let earlier = &self.venue_instruments[..i];
            if earlier.iter().any(|o| o.venue == vi.venue && o.instrument == vi.instrument) {
                return Err(RegistryError::DuplicateVenueInstrument(vi.venue, vi.instrument));
            }
            if earlier.iter().any(|o| o.venue == vi.venue && o.native_symbol == vi.native_symbol) {
                return Err(RegistryError::DuplicateNativeSymbol(vi.venue, vi.native_symbol.clone()));
            }
        }
        Ok(())
    }
}
