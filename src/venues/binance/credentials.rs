//! [CONTROL PLANE] Binance credentials, shared by all Binance markets.

use std::path::Path;

use zeroize::Zeroizing;

use crate::venues::Credentials;

/// Ed25519 API key, shared by all Binance markets.
const API_KEY_PATH: &str = "/etc/new_genesis/binance_api_key";

pub fn load_credentials() -> anyhow::Result<Credentials> {
    let key = std::fs::read_to_string(Path::new(API_KEY_PATH))
        .map_err(|e| anyhow::anyhow!("failed to read Binance API key {API_KEY_PATH}: {e}"))?;
    let key = key.trim();
    if key.is_empty() {
        anyhow::bail!("Binance API key is empty");
    }
    Ok(Credentials { api_key: Some(Zeroizing::new(key.to_owned())) })
}
