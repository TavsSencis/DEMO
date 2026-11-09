//! [DATA PLANE] DepthSnapshotStreamEvent, templateId 10002 (channel depth<N>@<symbol>).
//! Root block: eventTime i64, bookUpdateId i64, priceExponent i8, qtyExponent i8.
//! Groups (groupSize16Encoding): bids, asks; entry = price i64, qty i64.
//! Then data: symbol varString8.

use super::codec::{
    DecodeError, Group, GroupDims, LEVEL_LEN, SbeHeader, HEADER_LEN, ld_i8, ld_i64, read_group,
    read_var_string8, root_block,
};

pub const TEMPLATE_ID: u16 = 10002;
const MIN_BLOCK: usize = 18;

#[derive(Debug, Clone, Copy)]
pub struct DepthSnapshot<'a> {
    pub event_time_us: i64,
    pub book_update_id: i64,
    pub price_exponent: i8,
    pub qty_exponent: i8,
    pub bids: Group<'a>,
    pub asks: Group<'a>,
    pub symbol: &'a [u8],
}

#[inline(always)]
pub fn decode<'a>(msg: &'a [u8], hdr: &SbeHeader) -> Result<DepthSnapshot<'a>, DecodeError> {
    let end = root_block(msg, hdr, MIN_BLOCK)?;
    let (bids, end) = read_group(msg, end, GroupDims::Size16, LEVEL_LEN)?;
    let (asks, end) = read_group(msg, end, GroupDims::Size16, LEVEL_LEN)?;
    let (symbol, _) = read_var_string8(msg, end)?;
    let b = HEADER_LEN;
    // SAFETY: root_block checked HEADER_LEN + MIN_BLOCK <= msg.len().
    unsafe {
        Ok(DepthSnapshot {
            event_time_us: ld_i64(msg, b),
            book_update_id: ld_i64(msg, b + 8),
            price_exponent: ld_i8(msg, b + 16),
            qty_exponent: ld_i8(msg, b + 17),
            bids,
            asks,
            symbol,
        })
    }
}
