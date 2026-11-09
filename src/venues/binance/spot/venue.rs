//! [CONTROL PLANE] `BinanceSpot`: the `Venue` implementation (endpoints, channels, requests,
//! limits, connect, reference data, decoder construction).

use std::io;
use std::time::Duration;

use crate::control_plane::configs_struct::Environment;
use crate::control_plane::configs_struct::MarketDataType;
use crate::control_plane::startup::resolver::{ConnectionPlan, StartupPlan};
use crate::transport::tls;
use crate::transport::ws::WsConnection;
use crate::transport::ws::handshake;
use crate::venues::{Credentials, Endpoint, RestEndpoint, Venue, VenueLimits, VenuePair};

use super::decoder::BinanceSpotHandler;
use super::refdata;

pub struct BinanceSpot;

impl Venue for BinanceSpot {
    const LIMITS: VenueLimits = VenueLimits {
        max_streams_per_connection: 1024,
        max_requests_per_sec: 5,
        max_streams_per_request: 200,
    };
    type Handler = BinanceSpotHandler;

    fn endpoint(env: Environment) -> Option<Endpoint> {
        match env {
            Environment::Live => Some(Endpoint {
                addr: "stream-sbe.binance.com:443",
                host: "stream-sbe.binance.com",
                path: "/ws",
            }),
            Environment::Sandbox => Some(Endpoint {
                addr: "stream-sbe.testnet.binance.vision:443",
                host: "stream-sbe.testnet.binance.vision",
                path: "/ws",
            }),
        }
    }

    fn stream_name(native_symbol: &str, dt: MarketDataType) -> Option<String> {
        let channel = match dt {
            MarketDataType::Quote => "bestBidAsk",
            MarketDataType::Trade => "trade",
            // Only depth20 is used and verified on the SBE stream.
            MarketDataType::OrderBookSnapshot { depth: 20 } => "depth20",
            MarketDataType::OrderBookSnapshot { .. } => return None,
            MarketDataType::OrderBookDelta => "depth",
        };
        Some(format!("{}@{}", native_symbol.to_ascii_lowercase(), channel))
    }

    fn subscribe_request(streams: &[&str], id: u64) -> Vec<u8> {
        let params: Vec<String> = streams.iter().map(|s| format!("\"{s}\"")).collect();
        format!(r#"{{"method":"SUBSCRIBE","params":[{}],"id":{id}}}"#, params.join(",")).into_bytes()
    }

    fn connect(endpoint: &Endpoint, creds: &Credentials, timeout: Duration) -> io::Result<WsConnection> {
        let key = creds
            .api_key
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Binance SBE needs an API key"))?;
        let mut stream = tls::connect(endpoint.addr, endpoint.host, timeout)?;
        let pending = handshake::upgrade(&mut stream, endpoint.host, endpoint.path, &[("X-MBX-APIKEY", key.as_str())])?;
        Ok(WsConnection { stream, pending })
    }

    fn new_handler(_plan: &StartupPlan, conn: &ConnectionPlan) -> anyhow::Result<Self::Handler> {
        BinanceSpotHandler::new(conn)
    }

    fn refdata_endpoint(env: Environment) -> Option<RestEndpoint> {
        refdata::endpoint(env)
    }

    fn parse_instruments(body: &[u8]) -> anyhow::Result<Vec<VenuePair>> {
        refdata::parse(body)
    }
}
