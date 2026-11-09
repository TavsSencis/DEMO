//! [MIXED] OS access: `clock` = data plane (rdtsc); `memory`, `signal` = control plane.

pub mod clock;
pub mod memory;
pub mod signal;
