//! [CONTROL PLANE] Feed identity: the control plane's final product handed to the data plane.

/// Dense index of one (venue, instrument, market-data type) feed, assigned by the resolver.
/// The only identity the hot path sees; used as an array index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct FeedId(pub u16);

impl FeedId {
    #[inline(always)]
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}
