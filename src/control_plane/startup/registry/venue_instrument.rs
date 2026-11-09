//! [CONTROL PLANE] How one canonical instrument appears on one venue, as reported by the venue
//! itself (start-of-day reference data, see `control_plane::startup::refdata`). Cold-path data:
//! strings are fine here; the resolver turns it into channel names, route keys and exponents
//! before any connection exists.

use crate::control_plane::identity::{InstrumentId, VenueId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenueInstrument {
    pub venue: VenueId,
    pub instrument: InstrumentId,
    /// Symbol exactly as the venue writes it ("BTCUSDT" on Binance, "btcusdt" on Bitstamp).
    pub native_symbol: String,
    /// Fixed-point exponents (value = mantissa * 10^exp) derived from the venue's reported
    /// tick size / step size or decimals. Text venues parse at this scale and reject extra
    /// precision; binary venues (Binance SBE) carry their own exponent per message.
    pub price_exp: i8,
    pub qty_exp: i8,
    /// The venue reports the pair as tradable.
    pub trading: bool,
}
