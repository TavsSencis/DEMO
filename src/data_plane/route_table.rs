//! [DATA PLANE] Route key bytes (symbol or channel name) -> route slot, for one connection.
//! Built at startup, read-only after.
//!
//! Open addressing, 128 entries for <= 64 keys (load <= 50%), keys stored as four u64
//! words so a match is 4 integer compares. Whole table ~5 KiB, stays in L1.

use crate::control_plane::startup::resolver::plan::{MAX_ROUTE_KEY_LEN, MAX_ROUTE_KEYS_PER_CONNECTION};

const TABLE_SIZE: usize = 128;
const _: () = assert!(TABLE_SIZE.is_power_of_two() && TABLE_SIZE >= 2 * MAX_ROUTE_KEYS_PER_CONNECTION);

#[derive(Clone, Copy)]
#[repr(C)]
struct Entry {
    key: [u64; 4],
    /// 0 = empty slot.
    len: u8,
    slot: u8,
}

const EMPTY: Entry = Entry { key: [0; 4], len: 0, slot: 0 };

pub struct RouteTable {
    entries: Box<[Entry; TABLE_SIZE]>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RouteTableError {
    TooMany,
    BadKey(String),
    Duplicate(String),
}

/// Key words read straight from the key bytes with overlapping loads (no copy into a buffer:
/// a byte copy followed by word loads stalls store-to-load forwarding). Together with the
/// length (stored and compared next to them) the words cover every byte, so equal
/// (len, words) means equal keys. Same function at build and lookup.
#[inline(always)]
fn key_of(k: &[u8]) -> Option<[u64; 4]> {
    let n = k.len();
    let w64 = |b: &[u8]| b.first_chunk::<8>().map(|c| u64::from_le_bytes(*c));
    let w32 = |b: &[u8]| b.first_chunk::<4>().map(|c| u32::from_le_bytes(*c) as u64);
    Some(match n {
        1..=3 => [k[0] as u64 | (k[n / 2] as u64) << 8 | (k[n - 1] as u64) << 16, 0, 0, 0],
        4..=7 => [w32(k)? | w32(&k[n - 4..])? << 32, 0, 0, 0],
        8..=16 => [w64(k)?, w64(&k[n - 8..])?, 0, 0],
        17..=MAX_ROUTE_KEY_LEN => [w64(k)?, w64(&k[8..])?, w64(&k[n - 16..])?, w64(&k[n - 8..])?],
        _ => return None, // empty or longer than MAX_ROUTE_KEY_LEN
    })
}

#[inline(always)]
fn hash(key: &[u64; 4], len: u8) -> usize {
    let h = (key[0] ^ key[1].rotate_left(23) ^ key[2].rotate_left(41) ^ key[3] ^ len as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15);
    (h >> (64 - TABLE_SIZE.trailing_zeros())) as usize
}

impl RouteTable {
    /// `keys[i]` gets slot `i`.
    #[allow(clippy::disallowed_methods, clippy::disallowed_macros)] // control plane: runs once at startup
    pub fn new<S: AsRef<str>>(keys: &[S]) -> Result<Self, RouteTableError> {
        if keys.len() > MAX_ROUTE_KEYS_PER_CONNECTION {
            return Err(RouteTableError::TooMany);
        }
        let mut entries = Box::new([EMPTY; TABLE_SIZE]);
        for (slot, s) in keys.iter().map(|k| k.as_ref()).enumerate() {
            let key = key_of(s.as_bytes()).ok_or_else(|| RouteTableError::BadKey(s.to_string()))?;
            let len = s.len() as u8;
            let mut i = hash(&key, len);
            loop {
                let e = &mut entries[i];
                if e.len == 0 {
                    *e = Entry { key, len, slot: slot as u8 };
                    break;
                }
                if e.len == len && e.key == key {
                    return Err(RouteTableError::Duplicate(s.to_string()));
                }
                i = (i + 1) & (TABLE_SIZE - 1);
            }
        }
        Ok(Self { entries })
    }

    /// Hot path. `None` for keys this connection does not carry.
    #[inline(always)]
    pub fn lookup(&self, k: &[u8]) -> Option<usize> {
        let key = key_of(k)?;
        let len = k.len() as u8;
        let mut i = hash(&key, len);
        // Load <= 50% guarantees an empty slot, so this terminates; bound it anyway.
        for _ in 0..TABLE_SIZE {
            let e = &self.entries[i];
            if e.len == 0 {
                return None;
            }
            if e.len == len && e.key == key {
                return Some(e.slot as usize);
            }
            i = (i + 1) & (TABLE_SIZE - 1);
        }
        None
    }
}
