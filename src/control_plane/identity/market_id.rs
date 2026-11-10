//! [CONTROL PLANE] Market identity: one canonical instrument on one venue.

/// Dense index of one (venue, instrument) market, assigned by the resolver in first-appearance
/// order. Process-local. Carried by every market-data event; consumers index market state
/// (book, last trade) by it. All data types of a market share one `MarketId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct MarketId(pub u16);

impl MarketId {
    #[inline(always)]
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}
