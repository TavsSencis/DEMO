//! [DATA PLANE] Shared SBE primitives for Binance spot stream schema `spot_stream` (schemaId 1, version 0,
//! little endian). Source: binance-spot-api-docs/sbe/schemas/stream_1_0.xml.
//!
//! Decoding model: validate the whole message layout once (every length taken from the wire
//! is checked against the buffer), then read fields with unchecked unaligned loads.
//! Group and var-data offsets come from the wire `blockLength`s, so newer schema versions that
//! append fields still decode.

use core::ptr::read_unaligned;

pub const SCHEMA_ID: u16 = 1;
pub const HEADER_LEN: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SbeHeader {
    pub block_length: u16,
    pub template_id: u16,
    pub schema_id: u16,
    pub version: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    /// Buffer ends before the layout says it should.
    Truncated,
    WrongSchema(u16),
    UnknownTemplate(u16),
    /// A wire block length is smaller than the fields this decoder reads.
    BlockTooShort,
}

#[inline(always)]
pub fn read_header(msg: &[u8]) -> Result<SbeHeader, DecodeError> {
    if msg.len() < HEADER_LEN {
        return Err(DecodeError::Truncated);
    }
    // SAFETY: msg.len() >= 8 checked above.
    unsafe {
        Ok(SbeHeader {
            block_length: ld_u16(msg, 0),
            template_id: ld_u16(msg, 2),
            schema_id: ld_u16(msg, 4),
            version: ld_u16(msg, 6),
        })
    }
}

/// Checks that the root block starts in bounds and is at least `min_block` bytes.
/// Returns the offset just past the root block.
#[inline(always)]
pub fn root_block(msg: &[u8], hdr: &SbeHeader, min_block: usize) -> Result<usize, DecodeError> {
    let block = hdr.block_length as usize;
    if block < min_block {
        return Err(DecodeError::BlockTooShort);
    }
    let end = HEADER_LEN + block;
    if end > msg.len() {
        return Err(DecodeError::Truncated);
    }
    Ok(end)
}

/// Repeating group with fixed-size entries, already bounds-checked.
#[derive(Debug, Clone, Copy)]
pub struct Group<'a> {
    data: &'a [u8],
    stride: usize,
    count: usize,
}

impl<'a> Group<'a> {
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.count
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Entry `i` as a slice of at least `min_entry` bytes (as validated at decode time).
    #[inline(always)]
    pub fn entry(&self, i: usize) -> Option<&'a [u8]> {
        if i >= self.count {
            return None;
        }
        let off = i * self.stride;
        // SAFETY: data.len() == stride * count was validated, and i < count.
        Some(unsafe { self.data.get_unchecked(off..off + self.stride) })
    }
}

/// Group dimension encodings used by the schema.
#[derive(Clone, Copy)]
pub enum GroupDims {
    /// groupSizeEncoding: blockLength u16, numInGroup u32
    Size32,
    /// groupSize16Encoding: blockLength u16, numInGroup u16
    Size16,
}

/// Reads a group at `off`. Returns the group and the offset just past it.
#[inline(always)]
pub fn read_group(msg: &[u8], off: usize, dims: GroupDims, min_entry: usize) -> Result<(Group<'_>, usize), DecodeError> {
    let dims_len = match dims {
        GroupDims::Size32 => 6,
        GroupDims::Size16 => 4,
    };
    let data_off = off.checked_add(dims_len).ok_or(DecodeError::Truncated)?;
    if data_off > msg.len() {
        return Err(DecodeError::Truncated);
    }
    // SAFETY: off + dims_len <= msg.len() checked above.
    let (stride, count) = unsafe {
        let stride = ld_u16(msg, off) as usize;
        let count = match dims {
            GroupDims::Size32 => ld_u32(msg, off + 2) as usize,
            GroupDims::Size16 => ld_u16(msg, off + 2) as usize,
        };
        (stride, count)
    };
    if count > 0 && stride < min_entry {
        return Err(DecodeError::BlockTooShort);
    }
    let bytes = stride.checked_mul(count).ok_or(DecodeError::Truncated)?;
    let end = data_off.checked_add(bytes).ok_or(DecodeError::Truncated)?;
    if end > msg.len() {
        return Err(DecodeError::Truncated);
    }
    Ok((Group { data: &msg[data_off..end], stride, count }, end))
}

/// Reads a varString8 (length u8 + bytes) at `off`. Returns the bytes and the offset past them.
#[inline(always)]
pub fn read_var_string8(msg: &[u8], off: usize) -> Result<(&[u8], usize), DecodeError> {
    let len = *msg.get(off).ok_or(DecodeError::Truncated)? as usize;
    let start = off + 1;
    let end = start + len;
    if end > msg.len() {
        return Err(DecodeError::Truncated);
    }
    Ok((&msg[start..end], end))
}

// Unchecked little-endian loads. Callers guarantee `off + size_of::<T>() <= b.len()`.

#[inline(always)]
pub(crate) unsafe fn ld_u16(b: &[u8], off: usize) -> u16 {
    debug_assert!(off + 2 <= b.len());
    // SAFETY: caller guarantees bounds; read_unaligned has no alignment requirement.
    u16::from_le(unsafe { read_unaligned(b.as_ptr().add(off) as *const u16) })
}

#[inline(always)]
pub(crate) unsafe fn ld_u32(b: &[u8], off: usize) -> u32 {
    debug_assert!(off + 4 <= b.len());
    // SAFETY: as above.
    u32::from_le(unsafe { read_unaligned(b.as_ptr().add(off) as *const u32) })
}

#[inline(always)]
pub(crate) unsafe fn ld_i64(b: &[u8], off: usize) -> i64 {
    debug_assert!(off + 8 <= b.len());
    // SAFETY: as above.
    i64::from_le(unsafe { read_unaligned(b.as_ptr().add(off) as *const i64) })
}

#[inline(always)]
pub(crate) unsafe fn ld_i8(b: &[u8], off: usize) -> i8 {
    debug_assert!(off < b.len());
    // SAFETY: as above.
    unsafe { *b.get_unchecked(off) as i8 }
}

/// Price level entry in depth groups: price mantissa64, qty mantissa64.
pub const LEVEL_LEN: usize = 16;

/// Iterates (price_mantissa, qty_mantissa) pairs of a depth group.
#[inline(always)]
pub fn levels<'a>(g: Group<'a>) -> impl Iterator<Item = (i64, i64)> + 'a {
    (0..g.len()).map(move |i| {
        // entry() is Some for i < len, and each entry is >= LEVEL_LEN bytes (validated).
        let e = g.entry(i).unwrap_or(&[0u8; LEVEL_LEN]);
        // SAFETY: e.len() >= LEVEL_LEN.
        unsafe { (ld_i64(e, 0), ld_i64(e, 8)) }
    })
}
