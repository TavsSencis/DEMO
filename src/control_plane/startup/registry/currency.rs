//! [CONTROL PLANE] Canonical currencies / assets: Genesis's own identities, not venue data.
//! Derived at startup from the instruments in the node config; ids are dense and exist only
//! inside the process. Venue currency codes (e.g. Bitstamp "btc", a future venue's "XBT") are
//! normalized to these codes by each venue module.

use crate::control_plane::identity::CurrencyId;

#[derive(Debug, Clone)]
pub struct CurrencyDefinition {
    pub id: CurrencyId,
    /// Canonical upper-case code ("BTC", "USDT").
    pub code: String,
}
