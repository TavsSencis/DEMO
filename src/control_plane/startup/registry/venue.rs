//! [CONTROL PLANE] Venues as (exchange, market) pairs. Code, not config: each venue is a protocol
//! implementation in `venues/`. Config and the daily file name venues by `name`.

use crate::control_plane::identity::VenueId;

/// Which implementation serves a venue: exchange first, then its market.
/// Mirrors `venues/<exchange>/<market>/`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VenueKind {
    Binance(BinanceMarket),
    Bitstamp(BitstampMarket),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinanceMarket {
    /// Spot, SBE market-data streams.
    Spot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BitstampMarket {
    /// Spot, WebSocket API v2 (JSON).
    Spot,
}

/// One connectable venue = one (exchange, market) pair, e.g. BINANCE_SPOT.
#[derive(Debug, Clone, Copy)]
pub struct VenueDefinition {
    pub id: VenueId,
    pub name: &'static str,
    pub kind: VenueKind,
}

pub static VENUES: &[VenueDefinition] = &[
    VenueDefinition { id: VenueId(0), name: "BINANCE_SPOT", kind: VenueKind::Binance(BinanceMarket::Spot) },
    VenueDefinition { id: VenueId(1), name: "BITSTAMP_SPOT", kind: VenueKind::Bitstamp(BitstampMarket::Spot) },
];
