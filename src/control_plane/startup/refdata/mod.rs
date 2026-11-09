//! [CONTROL PLANE] Start-of-day reference data: what each venue calls the canonical instruments
//! and with which precision. `new_genesis refdata` reads the venues' REST APIs, maps their pairs
//! to Genesis instruments (base + quote currency + kind), validates and writes the daily file;
//! the trading node only loads that file.

pub mod build;
pub mod daily_file;

pub use build::build;
