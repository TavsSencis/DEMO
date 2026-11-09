//! [CONTROL PLANE] Lifecycle of one connection thread (the "conductor" for that connection).
//!
//! One connection = one thread for its whole life (unpinned; the CPU layout comes with the
//! production design):
//!   `prepare` (launch thread): decoder + subscriptions + `Session`; errors fail startup
//!   `run` (connection thread): allocate and prefault buffers -> loop {
//!       connect (TCP + TLS + WS upgrade, with timeout)
//!       switch socket to runtime mode (non-blocking; the reader busy-polls, never sleeps)
//!       session start: queue the subscription replay
//!       run the hot loop (`run_reader` + `Session`) until the session ends   <- DATA PLANE
//!       session end: feeds Down; reconnect at once after a healthy session (e.g. Binance
//!       24 h rotation) or a venue request, otherwise exponential backoff with jitter and an
//!       attempt budget
//!   }
//! Everything here runs outside the hot loop; allocation and clock calls are fine.

use std::collections::VecDeque;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::{Duration, Instant};

use anyhow::Context;
use openssl::ssl::SslStream;

use crate::control_plane::configs_struct::ReconnectPolicy;
use crate::control_plane::startup::resolver::StartupPlan;
use crate::control_plane::runtime::subscriptions::Subscriptions;
use crate::data_plane::events::EventOut;
use crate::data_plane::session::Session;
use crate::sys::clock::{TscClock, tsc};
use crate::transport::ws::WsConnection;
use crate::transport::ws::reader::{MAX_MESSAGE, ReaderBuffers, ReaderConfig, ReaderExit, run_reader};
use crate::transport::ws::writer::MaskGen;
use crate::venues::{Credentials, Endpoint, Venue};

pub struct ConnectionContext {
    pub plan: Arc<StartupPlan>,
    pub index: usize,
    pub endpoint: Endpoint,
    pub creds: Arc<Credentials>,
    pub out: EventOut,
    pub shutdown: Arc<AtomicBool>,
    pub clock: TscClock,
}

/// Builds everything a connection needs on the launch thread, before any thread starts:
/// decoder (route tables), subscription state, `Session`. Any error here fails `launch` (and
/// `Node::build`) with the connection's name. Returns the connection thread's body.
pub fn prepare<V: Venue>(ctx: ConnectionContext) -> anyhow::Result<impl FnOnce() + Send + 'static> {
    let ConnectionContext { plan, index, endpoint, creds, out, shutdown, clock } = ctx;
    let cp = &plan.connections[index];
    let handler = V::new_handler(&plan, cp).context("decoder")?; // the error names the connection
    let session = Session::<V>::new(
        handler,
        Subscriptions::new::<V>(&plan, cp),
        out,
        Arc::clone(&shutdown),
        V::HEARTBEAT.map_or(0, |hb| clock.ticks(hb.every).max(1)),
    );
    let reader_cfg = ReaderConfig { max_message: MAX_MESSAGE, stale_after_ticks: clock.ticks(plan.runtime.stale_timeout) };
    let policy = plan.runtime.reconnect;
    Ok(move || run::<V>(session, endpoint, creds, reader_cfg, policy, shutdown))
}

/// Connection thread body: connect / session / reconnect until shutdown.
fn run<V: Venue>(
    mut session: Session<V>,
    endpoint: Endpoint,
    creds: Arc<Credentials>,
    reader_cfg: ReaderConfig,
    policy: ReconnectPolicy,
    shutdown: Arc<AtomicBool>,
) {
    // Allocated and prefaulted on the connection thread itself (first touch = its memory).
    let mut bufs = ReaderBuffers::default();
    let mut attempts = AttemptWindow::new(policy.max_attempts_per_5min as usize, Duration::from_secs(300));
    let mut backoff = policy.initial_backoff;
    let mut rng = tsc() | 1;

    while !shutdown.load(Relaxed) {
        let now = Instant::now();
        if let Some(wait) = attempts.wait_needed(now) {
            sleep_unless_shutdown(wait, &shutdown);
            continue;
        }
        attempts.record(now);

        let connected = V::connect(&endpoint, &creds, policy.connect_timeout).and_then(|c| {
            configure_socket(&c.stream)?;
            Ok(c)
        });

        // A failed connect falls through to the backoff below.
        if let Ok(WsConnection { mut stream, pending }) = connected {
            let started = Instant::now();
            let exit = match MaskGen::from_os_entropy() {
                Ok(mut mask) => {
                    session.on_session_start();
                    // DATA PLANE: runs until the session ends.
                    run_reader(&mut stream, &pending, &mut mask, &mut bufs, &reader_cfg, &mut session)
                }
                Err(e) => ReaderExit::Io(e),
            };
            session.on_session_end();
            let lived = started.elapsed();
            if matches!(exit, ReaderExit::Shutdown) {
                break;
            }
            if matches!(exit, ReaderExit::VenueRequestedReconnect) || lived >= policy.healthy_after {
                backoff = policy.initial_backoff;
                continue;
            }
        }

        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let half = backoff / 2;
        let jitter = Duration::from_nanos(rng % (half.as_nanos() as u64).max(1));
        sleep_unless_shutdown(half + jitter, &shutdown);
        backoff = (backoff * 2).min(policy.max_backoff);
    }
}

/// Switches a freshly upgraded socket from handshake mode (blocking with timeouts) to runtime
/// mode: non-blocking, so the reader busy-polls and never sleeps in the kernel.
fn configure_socket(stream: &SslStream<TcpStream>) -> std::io::Result<()> {
    stream.get_ref().set_nonblocking(true)
}

fn sleep_unless_shutdown(d: Duration, shutdown: &AtomicBool) {
    let end = Instant::now() + d;
    while !shutdown.load(Relaxed) {
        let now = Instant::now();
        if now >= end {
            break;
        }
        std::thread::sleep((end - now).min(Duration::from_millis(50)));
    }
}

/// Rolling-window budget of connection attempts.
struct AttemptWindow {
    times: VecDeque<Instant>,
    max: usize,
    window: Duration,
}

impl AttemptWindow {
    fn new(max: usize, window: Duration) -> Self {
        Self { times: VecDeque::with_capacity(max), max: max.max(1), window }
    }

    fn wait_needed(&mut self, now: Instant) -> Option<Duration> {
        while self.times.front().is_some_and(|t| now.duration_since(*t) >= self.window) {
            self.times.pop_front();
        }
        if self.times.len() < self.max {
            return None;
        }
        self.times.front().map(|t| self.window - now.duration_since(*t))
    }

    fn record(&mut self, now: Instant) {
        self.times.push_back(now);
    }
}
