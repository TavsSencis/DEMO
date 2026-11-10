//! [CONTROL PLANE] Canonical instrument identity.

/// Genesis instrument identity (canonical, venue-independent).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct InstrumentId(pub u16);
