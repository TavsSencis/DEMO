//! [CONTROL PLANE] Errors of startup resolution. Every configuration or reference-data mistake
//! surfaces here, before any connection exists.

use std::fmt;

use crate::control_plane::configs_struct::MarketDataType;
use crate::control_plane::identity::{InstrumentId, VenueId};
use crate::control_plane::startup::registry::{RegistryError, VenueKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanError {
    Registry(RegistryError),
    NoFeeds,
    UnknownVenue(VenueId),
    /// The daily reference-data file has no such pair for this venue.
    NotListed(VenueId, InstrumentId),
    /// The venue reports the pair as not tradable.
    NotTrading(VenueId, InstrumentId),
    EmptySubscriptions(VenueId, InstrumentId),
    DuplicateFeed(VenueId, InstrumentId, MarketDataType),
    Unsupported(VenueKind, MarketDataType),
    RouteKeyTooLong(String),
    TooManyFeeds,
    ZeroDuration(&'static str),
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "startup plan: {self:?}")
    }
}

impl std::error::Error for PlanError {}

impl From<RegistryError> for PlanError {
    fn from(e: RegistryError) -> Self {
        PlanError::Registry(e)
    }
}
