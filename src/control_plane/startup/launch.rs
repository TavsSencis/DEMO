//! [CONTROL PLANE] Turns a `StartupPlan` into running connection threads. Control plane.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::thread::{self, JoinHandle};

use anyhow::{anyhow, bail};

use crate::control_plane::startup::registry::VenueKind;
use crate::control_plane::startup::resolver::StartupPlan;
use crate::control_plane::runtime::supervisor::ConnectionContext;
use crate::data_plane::events::{EventOut, MdConsumer};
use crate::data_plane::spsc::SpscRing;
use crate::sys::clock::TscClock;
use crate::venues::{self, Credentials};

/// Handle to the running market-data layer.
pub struct MarketData {
    /// One event stream per connection (index = connection index). Each has one consumer.
    pub events: Vec<MdConsumer>,
    shutdown: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
}

impl MarketData {
    /// Stops every connection thread and waits for them.
    pub fn shutdown(self) {
        self.shutdown.store(true, Relaxed);
        for t in self.threads {
            let _ = t.join();
        }
    }
}

pub fn launch(plan: StartupPlan) -> anyhow::Result<MarketData> {
    let plan = Arc::new(plan);
    let clock = TscClock::calibrate();

    // Credentials once per venue kind; endpoints checked before any thread starts.
    let mut creds: Vec<(VenueKind, Arc<Credentials>)> = Vec::new();
    for c in &plan.connections {
        if venues::endpoint(c.venue_kind, plan.environment).is_none() {
            bail!("{}: no endpoint for {:?}", c.name, plan.environment);
        }
        if !creds.iter().any(|(k, _)| *k == c.venue_kind) {
            creds.push((c.venue_kind, Arc::new(venues::load_credentials(c.venue_kind)?)));
        }
    }

    // Pass 1: prepare every connection (decoder, session). An error returns here, before any
    // thread exists.
    let shutdown = Arc::new(AtomicBool::new(false));
    let mut events = Vec::new();
    let mut bodies = Vec::new();
    for c in &plan.connections {
        let (ev_tx, ev_rx) = SpscRing::pair();
        let ctx = ConnectionContext {
            plan: Arc::clone(&plan),
            index: c.index,
            endpoint: venues::endpoint(c.venue_kind, plan.environment).ok_or_else(|| anyhow!("no endpoint"))?,
            creds: Arc::clone(&creds.iter().find(|(k, _)| *k == c.venue_kind).expect("loaded above").1),
            out: EventOut::new(ev_tx),
            shutdown: Arc::clone(&shutdown),
            clock,
        };
        bodies.push(venues::prepare_connection(c.venue_kind, ctx)?);
        events.push(ev_rx);
    }

    // Pass 2: one thread per connection.
    let mut threads = Vec::new();
    for (c, body) in plan.connections.iter().zip(bodies) {
        let spawned = thread::Builder::new().name(c.thread_name.clone()).spawn(body);
        match spawned {
            Ok(h) => threads.push(h),
            Err(e) => {
                shutdown.store(true, Relaxed);
                for t in threads {
                    let _ = t.join();
                }
                return Err(anyhow!("spawning {}: {e}", c.name));
            }
        }
    }

    Ok(MarketData { events, shutdown, threads })
}
