//! [CONTROL PLANE] The daily reference-data file: written by `new_genesis refdata`, loaded by the
//! trading node at startup. One JSON file per UTC day (`refdata-YYYY-MM-DD.json`), written
//! atomically (temp file + rename) so a reader never sees a half-written file.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};

use crate::control_plane::configs_struct::Environment;
use crate::control_plane::startup::registry::{InstrumentKey, Registry, VenueInstrument};

/// v3: entries name venues and instruments canonically ("BINANCE_SPOT", "BTC/USDT"); numeric
/// ids exist only inside a process, so the file can never point at the wrong instrument.
pub const FORMAT_VERSION: u32 = 3;
const PREFIX: &str = "refdata-";
const SUFFIX: &str = ".json";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefDataFile {
    pub format_version: u32,
    /// Unix seconds when the venues were read.
    pub generated_at_unix: u64,
    pub environment: Environment,
    pub instruments: Vec<RefDataEntry>,
}

/// How one canonical instrument appears on one venue, by canonical names.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefDataEntry {
    /// Venue name, e.g. "BINANCE_SPOT".
    pub venue: String,
    /// Canonical instrument, e.g. "BTC/USDT".
    pub instrument: String,
    /// Symbol exactly as the venue writes it ("BTCUSDT", "btcusdt").
    pub native_symbol: String,
    pub price_exp: i8,
    pub qty_exp: i8,
    pub trading: bool,
}

impl RefDataFile {
    /// Entries for the instruments and venues in `reg`, with the process's numeric ids.
    /// Entries for instruments the node does not use are skipped; a configured pair missing
    /// from the file is reported by the resolver (`NotListed`).
    pub fn venue_instruments(&self, reg: &Registry) -> Vec<VenueInstrument> {
        self.instruments
            .iter()
            .filter_map(|e| {
                let venue = reg.venue_by_name(&e.venue)?.id;
                let instrument = reg.instrument_by_key(&InstrumentKey::parse(&e.instrument)?)?.id;
                Some(VenueInstrument {
                    venue,
                    instrument,
                    native_symbol: e.native_symbol.clone(),
                    price_exp: e.price_exp,
                    qty_exp: e.qty_exp,
                    trading: e.trading,
                })
            })
            .collect()
    }
}

pub fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// Writes `refdata-YYYY-MM-DD.json` (UTC date of `generated_at_unix`) into `dir`.
pub fn save(dir: &Path, file: &RefDataFile) -> anyhow::Result<PathBuf> {
    fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    let (y, m, d) = utc_date(file.generated_at_unix);
    let path = dir.join(format!("{PREFIX}{y:04}-{m:02}-{d:02}{SUFFIX}"));
    let tmp = dir.join(format!(".{PREFIX}{}.tmp", std::process::id()));
    let json = serde_json::to_vec_pretty(file).context("serialize reference data")?;
    {
        let mut f = fs::File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
        f.write_all(&json)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, &path).with_context(|| format!("rename to {}", path.display()))?;
    Ok(path)
}

/// Loads the newest daily file for `env`, rejecting one older than `max_age`.
pub fn load_latest(dir: &Path, env: Environment, max_age: Duration) -> anyhow::Result<RefDataFile> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .with_context(|| format!("no reference data in {} (run `new_genesis refdata`)", dir.display()))?
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter(|n| n.starts_with(PREFIX) && n.ends_with(SUFFIX))
        .collect();
    // ISO dates sort lexicographically.
    names.sort();
    let newest = names
        .last()
        .with_context(|| format!("no reference data in {} (run `new_genesis refdata`)", dir.display()))?;
    let path = dir.join(newest);
    let bytes = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
    let file: RefDataFile = serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))?;
    if file.format_version != FORMAT_VERSION {
        bail!("{}: format version {} (expected {FORMAT_VERSION})", path.display(), file.format_version);
    }
    if file.environment != env {
        bail!("{}: written for {:?}, node runs {:?}", path.display(), file.environment, env);
    }
    let age = now_unix().saturating_sub(file.generated_at_unix);
    if age > max_age.as_secs() {
        bail!(
            "{}: {} h old, limit {} h (run `new_genesis refdata`)",
            path.display(),
            age / 3600,
            max_age.as_secs() / 3600
        );
    }
    Ok(file)
}

/// UTC calendar date of a Unix timestamp (civil-from-days algorithm).
fn utc_date(unix: u64) -> (i64, u32, u32) {
    let z = (unix / 86_400) as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}
