//! [CONTROL PLANE] Stream identity: one subscribed venue stream or channel.

/// Dense index of one (venue, instrument, market-data type) subscription, assigned by the
/// resolver in config order. Control plane only: subscription state (sent, Up, Down, Failed),
/// acks and retries. Market-data events carry `MarketId` + `InstrumentId` instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct StreamId(pub u16);

impl StreamId {
    #[inline(always)]
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}
