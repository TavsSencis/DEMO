//! [CONTROL PLANE] Venue identity.

/// Genesis venue identity: one (exchange, market) pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct VenueId(pub u16);
