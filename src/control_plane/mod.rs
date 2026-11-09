//! [CONTROL PLANE] Everything that is not per-message work. Allocation, strings, clocks and
//! sleeping are allowed here. The hot path lives in `data_plane`.
//!
//! - `startup/`: runs once before any connection thread: config file, reference data, registry,
//!   resolver (StartupPlan), launch.
//! - `runtime/`: runs on the connection threads while the node is live, per session or
//!   amortized: supervisor (connect / reconnect), subscriptions.
//! - `identity/`, `configs_struct.rs`  types shared by both phases (and the data plane).
//! - `node.rs`   ties both: `Node::build` (startup) -> running node -> `shutdown`.

pub mod configs_struct;
pub mod identity;
pub mod node;
pub mod runtime;
pub mod startup;
