//! Genesis: ULL multi-venue crypto market-data infrastructure.
//!
//! Two planes:
//! - `control_plane` (cold): config, registry, resolver, launch, supervision, subscriptions.
//! - `data_plane` (hot): per-message session, events, rings, routing, parsing helpers.
//!
//! Shared by both, each file tagged with its plane: `venues` (`mod.rs` = control,
//! `decode.rs`/`decoder.rs`/`sbe` = data), `transport` (TCP/TLS/WebSocket), `sys` (OS).
//
// The control plane may allocate; the data plane denies these lints in its own modules.
#![allow(clippy::disallowed_methods, clippy::disallowed_macros)]

pub mod control_plane;
pub mod data_plane;
pub mod sys;
pub mod transport;
pub mod venues;
