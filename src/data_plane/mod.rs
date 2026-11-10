//! [DATA PLANE] Code that runs for every message, on the connection threads:
//! `session` (per-message dispatch), normalized `events` and their SPSC rings, routing
//! (`route_table`) and zero-allocation parsing helpers (`decimal`, `json_scan`).
//!
//! Rules (enforced by clippy, see `clippy.toml`): no heap allocation, formatting, printing,
//! locks, sleeping or clock syscalls. Startup constructors are explicitly allowed one by one.
//! Control-plane code is reached only from `#[cold]` branches or amortized housekeeping.
#![deny(clippy::disallowed_methods, clippy::disallowed_macros)]

pub mod decimal;
pub mod events;
pub mod json_scan;
pub mod route_table;
pub mod session;
pub mod spsc;
