//! [CONTROL PLANE] The `Venue` trait: everything a market module provides to the control plane
//! (endpoints, channel names, route keys, requests, limits, keepalive, connect, reference data,
//! decoder construction) and the small types it uses.

use std::io;
use std::time::Duration;

use zeroize::Zeroizing;

use crate::control_plane::configs_struct::{Environment, MarketDataType};
use crate::control_plane::startup::resolver::{ConnectionPlan, StartupPlan};
use crate::transport::ws::WsConnection;
use crate::venues::decode::VenueHandler;
use crate::venues::venue_pair::VenuePair;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Endpoint {
    pub addr: &'static str,
    pub host: &'static str,
    pub path: &'static str,
}

/// HTTPS endpoint of a venue's REST API (start-of-day reference data only).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestEndpoint {
    pub addr: &'static str,
    pub host: &'static str,
    /// Path including any query string.
    pub path: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VenueLimits {
    pub max_streams_per_connection: usize,
    /// Subscribe requests we send per second per connection.
    pub max_requests_per_sec: u32,
    /// Channels per request (1 for venues that take one channel per message).
    pub max_streams_per_request: usize,
}

/// Application-level keepalive sent by the client (for venues that need it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Heartbeat {
    pub payload: &'static [u8],
    pub every: Duration,
}

#[derive(Default)]
pub struct Credentials {
    pub api_key: Option<Zeroizing<String>>,
}

pub trait Venue: 'static {
    const LIMITS: VenueLimits;
    /// Client keepalive, if the venue needs one (server pings are answered by the transport).
    const HEARTBEAT: Option<Heartbeat> = None;
    type Handler: VenueHandler;

    fn endpoint(env: Environment) -> Option<Endpoint>;
    /// Channel name for one (symbol, data type), or `None` if unsupported.
    fn stream_name(native_symbol: &str, dt: MarketDataType) -> Option<String>;
    /// Bytes the decoder uses to identify the feed in an inbound message: the symbol by
    /// default (Binance carries it in each message); venues that only name the channel
    /// (Bitstamp) return the channel name.
    fn route_key(native_symbol: &str, _dt: MarketDataType) -> String {
        native_symbol.to_string()
    }
    /// Wire SUBSCRIBE request for `streams`. `id` may be ignored by venues without request ids.
    fn subscribe_request(streams: &[&str], id: u64) -> Vec<u8>;
    fn connect(endpoint: &Endpoint, creds: &Credentials, timeout: Duration) -> io::Result<WsConnection>;
    fn new_handler(plan: &StartupPlan, conn: &ConnectionPlan) -> anyhow::Result<Self::Handler>;

    // ── Start-of-day reference data (cold path, `new_genesis refdata`) ──

    /// REST endpoint listing the venue's instruments, if the venue offers one for `env`.
    fn refdata_endpoint(env: Environment) -> Option<RestEndpoint>;
    /// Parses that endpoint's response into normalized pairs.
    fn parse_instruments(body: &[u8]) -> anyhow::Result<Vec<VenuePair>>;
    /// Venue currency code -> canonical Genesis code. Default: upper-case. Venues with
    /// aliases (e.g. "XBT" for BTC) override this.
    fn currency_code(code: &str) -> String {
        code.trim().to_ascii_uppercase()
    }
}
