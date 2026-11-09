//! [CONTROL PLANE] Canonical currency / asset identity.

/// Canonical currency / asset identity (BTC, USDT, USD, ...), venue-independent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct CurrencyId(pub u16);
