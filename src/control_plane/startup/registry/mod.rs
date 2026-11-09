//! [CONTROL PLANE] Registry: WHAT exists. Canonical definitions (currencies and instruments
//! derived from the node config, venues from code), how instruments appear on venues (from the daily reference-data file), and the
//! `Registry` catalog that the resolver reads at startup. Identities live in `identity`.
//! The hot path never touches this module.

pub mod catalog;
pub mod currency;
pub mod error;
pub mod instrument;
pub mod validation;
pub mod venue;
pub mod venue_instrument;

pub use catalog::Registry;
pub use currency::CurrencyDefinition;
pub use error::RegistryError;
pub use instrument::{InstrumentDefinition, InstrumentKey, InstrumentKind};
pub use venue::{VenueDefinition, VenueKind};
pub use venue_instrument::VenueInstrument;
