//! [CONTROL PLANE] `BitstampSpot`: the `Venue` implementation (endpoints, channels, requests,
//! limits, connect, reference data, decoder construction).

use std::io;
use std::time::Duration;

use crate::control_plane::configs_struct::Environment;
use crate::control_plane::configs_struct::MarketDataType;
use crate::control_plane::startup::resolver::{ConnectionPlan, StartupPlan};
use crate::transport::tls;
use crate::transport::ws::WsConnection;
use crate::transport::ws::handshake;
use crate::venues::{Credentials, Endpoint, Heartbeat, RestEndpoint, Venue, VenueLimits, VenuePair};

use super::decoder::BitstampSpotHandler;
use super::refdata;

pub struct BitstampSpot;

impl Venue for BitstampSpot {
    const LIMITS: VenueLimits = VenueLimits {
        // Documented: up to 1024 channels per connection (the 1025th closes it).
        max_streams_per_connection: 1024,
        // No documented request rate; our own conservative cap.
        max_requests_per_sec: 10,
        // One channel per subscribe message.
        max_streams_per_request: 1,
    };
    // Keeps quiet channels from looking stale; the server answers every heartbeat.
    const HEARTBEAT: Option<Heartbeat> =
        Some(Heartbeat { payload: br#"{"event":"bts:heartbeat"}"#, every: Duration::from_secs(10) });
    type Handler = BitstampSpotHandler;

    fn endpoint(env: Environment) -> Option<Endpoint> {
        match env {
            Environment::Live => Some(Endpoint { addr: "ws.bitstamp.net:443", host: "ws.bitstamp.net", path: "/" }),
            // No public sandbox WebSocket.
            Environment::Sandbox => None,
        }
    }

    fn stream_name(native_symbol: &str, dt: MarketDataType) -> Option<String> {
        let prefix = match dt {
            MarketDataType::Trade => "live_trades_",
            // order_book_* is a full top-100 snapshot on every change.
            MarketDataType::OrderBookSnapshot { depth: 100 } => "order_book_",
            MarketDataType::OrderBookDelta => "diff_order_book_",
            // No best-bid/ask channel.
            MarketDataType::Quote | MarketDataType::OrderBookSnapshot { .. } => return None,
        };
        Some(format!("{prefix}{native_symbol}"))
    }

    /// Bitstamp messages name the channel, not a symbol field, so the channel is the key.
    fn route_key(native_symbol: &str, dt: MarketDataType) -> String {
        Self::stream_name(native_symbol, dt).unwrap_or_default()
    }

    fn subscribe_request(streams: &[&str], _id: u64) -> Vec<u8> {
        debug_assert_eq!(streams.len(), 1, "Bitstamp takes one channel per request");
        format!(r#"{{"event":"bts:subscribe","data":{{"channel":"{}"}}}}"#, streams.first().copied().unwrap_or_default())
            .into_bytes()
    }

    fn connect(endpoint: &Endpoint, _creds: &Credentials, timeout: Duration) -> io::Result<WsConnection> {
        let mut stream = tls::connect(endpoint.addr, endpoint.host, timeout)?;
        let pending = handshake::upgrade(&mut stream, endpoint.host, endpoint.path, &[])?;
        Ok(WsConnection { stream, pending })
    }

    fn new_handler(plan: &StartupPlan, conn: &ConnectionPlan) -> anyhow::Result<Self::Handler> {
        BitstampSpotHandler::new(plan, conn)
    }

    fn refdata_endpoint(env: Environment) -> Option<RestEndpoint> {
        refdata::endpoint(env)
    }

    fn parse_instruments(body: &[u8]) -> anyhow::Result<Vec<VenuePair>> {
        refdata::parse(body)
    }
}
