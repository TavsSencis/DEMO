//! [DATA PLANE] BestBidAskStreamEvent, templateId 10001.
//! Root block: eventTime i64, bookUpdateId i64, priceExponent i8, qtyExponent i8,
//! bidPrice i64, bidQty i64, askPrice i64, askQty i64. Then data: symbol varString8.

use super::codec::{DecodeError, SbeHeader, ld_i8, ld_i64, read_var_string8, root_block, HEADER_LEN};

pub const TEMPLATE_ID: u16 = 10001;
const MIN_BLOCK: usize = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BestBidAsk<'a> {
    pub event_time_us: i64,
    pub book_update_id: i64,
    pub price_exponent: i8,
    pub qty_exponent: i8,
    pub bid_price: i64,
    pub bid_qty: i64,
    pub ask_price: i64,
    pub ask_qty: i64,
    pub symbol: &'a [u8],
}

#[inline(always)]
pub fn decode<'a>(msg: &'a [u8], hdr: &SbeHeader) -> Result<BestBidAsk<'a>, DecodeError> {
    let end = root_block(msg, hdr, MIN_BLOCK)?;
    let (symbol, _) = read_var_string8(msg, end)?;
    let b = HEADER_LEN;
    // SAFETY: root_block checked HEADER_LEN + MIN_BLOCK <= msg.len().
    unsafe {
        Ok(BestBidAsk {
            event_time_us: ld_i64(msg, b),
            book_update_id: ld_i64(msg, b + 8),
            price_exponent: ld_i8(msg, b + 16),
            qty_exponent: ld_i8(msg, b + 17),
            bid_price: ld_i64(msg, b + 18),
            bid_qty: ld_i64(msg, b + 26),
            ask_price: ld_i64(msg, b + 34),
            ask_qty: ld_i64(msg, b + 42),
            symbol,
        })
    }
}
