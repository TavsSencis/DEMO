//! [DATA PLANE] TradesStreamEvent, templateId 10000 (channel trade@<symbol>).
//! Root block: eventTime i64, transactTime i64, priceExponent i8, qtyExponent i8.
//! Group trades (groupSizeEncoding: u16 blockLength, u32 numInGroup);
//! entry = id i64, price i64, qty i64, isBuyerMaker u8 (isBestMatch is constant, not on the wire).
//! Then data: symbol varString8.

use super::codec::{
    DecodeError, Group, GroupDims, SbeHeader, HEADER_LEN, ld_i8, ld_i64, read_group,
    read_var_string8, root_block,
};

pub const TEMPLATE_ID: u16 = 10000;
const MIN_BLOCK: usize = 18;
const TRADE_ENTRY_LEN: usize = 25;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trade {
    pub id: i64,
    pub price: i64,
    pub qty: i64,
    pub is_buyer_maker: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct Trades<'a> {
    pub event_time_us: i64,
    pub transact_time_us: i64,
    pub price_exponent: i8,
    pub qty_exponent: i8,
    pub trades: Group<'a>,
    pub symbol: &'a [u8],
}

impl<'a> Trades<'a> {
    #[inline(always)]
    pub fn iter(&self) -> impl Iterator<Item = Trade> + 'a {
        let g = self.trades;
        (0..g.len()).filter_map(move |i| {
            let e = g.entry(i)?;
            // SAFETY: each entry is >= TRADE_ENTRY_LEN bytes (validated in decode).
            unsafe {
                Some(Trade {
                    id: ld_i64(e, 0),
                    price: ld_i64(e, 8),
                    qty: ld_i64(e, 16),
                    is_buyer_maker: *e.get_unchecked(24) != 0,
                })
            }
        })
    }
}

#[inline(always)]
pub fn decode<'a>(msg: &'a [u8], hdr: &SbeHeader) -> Result<Trades<'a>, DecodeError> {
    let end = root_block(msg, hdr, MIN_BLOCK)?;
    let (trades, end) = read_group(msg, end, GroupDims::Size32, TRADE_ENTRY_LEN)?;
    let (symbol, _) = read_var_string8(msg, end)?;
    let b = HEADER_LEN;
    // SAFETY: root_block checked HEADER_LEN + MIN_BLOCK <= msg.len().
    unsafe {
        Ok(Trades {
            event_time_us: ld_i64(msg, b),
            transact_time_us: ld_i64(msg, b + 8),
            price_exponent: ld_i8(msg, b + 16),
            qty_exponent: ld_i8(msg, b + 17),
            trades,
            symbol,
        })
    }
}
