//! [CONTROL PLANE] Binary entry. Both commands read the node config file
//! (`config/genesis.toml`, or the path in `GENESIS_CONFIG`).
//!   `new_genesis refdata`  start-of-day: read the venues' reference data, write the daily file
//!   `new_genesis`          trading: build the node from the config, run until Ctrl-C
#![allow(clippy::disallowed_methods, clippy::disallowed_macros)]

use anyhow::bail;
use new_genesis::control_plane::configs_struct::NodeConfig;
use new_genesis::control_plane::node::Node;
use new_genesis::control_plane::startup::registry::Registry;
use new_genesis::control_plane::startup::{config_file, refdata};
use new_genesis::sys::signal;

mod consumer;

fn main() -> anyhow::Result<()> {
    let (config, registry) = config_file::load(&config_file::path())?;
    match std::env::args().nth(1).as_deref() {
        None => run_node(config, registry),
        Some("refdata") => {
            // Success = exit code 0 and the dated file in `config.refdata.dir`.
            let file = refdata::build(config.environment, &registry)?;
            refdata::daily_file::save(&config.refdata.dir, &file)?;
            Ok(())
        }
        Some(other) => bail!("unknown command {other:?} (expected `refdata` or nothing)"),
    }
}

fn run_node(config: NodeConfig, registry: Registry) -> anyhow::Result<()> {
    signal::install_stop_handler();

    // Control plane: config + daily reference data -> validated plan -> connection threads.
    let mut node = Node::build(config, registry)?;
    // Data plane runs on the connection threads; this thread is the placeholder consumer.
    consumer::run(node.market_data(), signal::stop_requested);

    node.shutdown();
    Ok(())
}
