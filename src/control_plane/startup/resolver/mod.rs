//! [CONTROL PLANE] Resolver: HOW the node configuration becomes the startup plan.
//! `resolve` (the algorithm), `plan` (its immutable output), `error` (what can go wrong).

pub mod error;
pub mod plan;
pub mod resolve;

pub use error::PlanError;
pub use plan::{ConnectionPlan, MarketSpec, RouteTarget, StartupPlan, StreamSpec};
pub use resolve::resolve;
