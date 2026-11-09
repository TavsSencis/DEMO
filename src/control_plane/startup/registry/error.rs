//! [CONTROL PLANE] Errors found by `Registry::validate`.

use std::fmt;

use crate::control_plane::identity::{CurrencyId, InstrumentId, VenueId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    DuplicateCurrency(CurrencyId),
    DuplicateInstrument(InstrumentId),
    DuplicateVenue(VenueId),
    /// An instrument refers to a currency that is not defined.
    UnknownCurrency(InstrumentId),
    VenueInstrumentUnknownVenue(VenueId),
    VenueInstrumentUnknownInstrument(InstrumentId),
    DuplicateVenueInstrument(VenueId, InstrumentId),
    DuplicateNativeSymbol(VenueId, String),
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "registry: {self:?}")
    }
}

impl std::error::Error for RegistryError {}
