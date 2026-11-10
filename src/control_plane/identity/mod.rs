//! [CONTROL PLANE] Identity: WHAT a thing is. Dense numeric identities used across Genesis,
//! independent of any venue's names; all process-local. Definitions of what exists live in
//! `registry`. Market-data events carry `MarketId` + `InstrumentId`; `StreamId` stays on the
//! control plane (subscription state).

pub mod currency_id;
pub mod instrument_id;
pub mod market_id;
pub mod stream_id;
pub mod venue_id;

pub use currency_id::CurrencyId;
pub use instrument_id::InstrumentId;
pub use market_id::MarketId;
pub use stream_id::StreamId;
pub use venue_id::VenueId;
