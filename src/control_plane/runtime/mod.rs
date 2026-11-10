//! [CONTROL PLANE] Runtime, cold side: runs on each connection thread while the node is live,
//! per session or amortized, never per message. `supervisor` = connect / reconnect loop,
//! `subscriptions` = subscribe, acks, timeouts, retries. Per-message work is in `data_plane`.

pub mod stream_set;
pub mod subscriptions;
pub mod supervisor;
