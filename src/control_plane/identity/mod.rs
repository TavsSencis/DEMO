//! [CONTROL PLANE] Identity: WHAT a thing is. Dense numeric identities used across Genesis,
//! independent of any venue's names. Definitions of what exists live in `registry`.
//! `FeedId` is the only identity the data plane uses (as an array index).

pub mod currency_id;
pub mod feed_id;
pub mod instrument_id;
pub mod venue_id;

pub use currency_id::CurrencyId;
pub use feed_id::FeedId;
pub use instrument_id::InstrumentId;
pub use venue_id::VenueId;
