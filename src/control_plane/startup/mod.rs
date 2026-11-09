//! [CONTROL PLANE] Startup: runs once in `Node::build` (and in the `refdata` command), never
//! after the connection threads start.
//!
//! Flow: `config_file` (TOML: what to run) -> `refdata` (daily venue file) -> `registry` (what exists)
//! -> `resolver` (validated `StartupPlan`) -> `launch` (prepare connections, spawn threads).

pub mod config_file;
pub mod launch;
pub mod refdata;
pub mod registry;
pub mod resolver;
