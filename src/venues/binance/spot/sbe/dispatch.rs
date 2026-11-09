//! [DATA PLANE] templateId -> decoder; returns zero-copy views of the receive buffer.

use super::best_bidask::{self, BestBidAsk};
use super::depth_20::{self, DepthSnapshot};
use super::depth_diff::{self, DepthDiff};
use super::codec::{DecodeError, SCHEMA_ID, read_header};
use super::trade::{self, Trades};

/// One decoded Binance SBE stream message. Borrows the receive buffer (zero copy).
#[derive(Debug, Clone, Copy)]
pub enum BinanceSbeMsg<'a> {
    Trades(Trades<'a>),
    BestBidAsk(BestBidAsk<'a>),
    DepthSnapshot(DepthSnapshot<'a>),
    DepthDiff(DepthDiff<'a>),
}

#[inline(always)]
pub fn decode(msg: &[u8]) -> Result<BinanceSbeMsg<'_>, DecodeError> {
    let hdr = read_header(msg)?;
    if hdr.schema_id != SCHEMA_ID {
        return Err(DecodeError::WrongSchema(hdr.schema_id));
    }
    match hdr.template_id {
        best_bidask::TEMPLATE_ID => best_bidask::decode(msg, &hdr).map(BinanceSbeMsg::BestBidAsk),
        depth_20::TEMPLATE_ID => depth_20::decode(msg, &hdr).map(BinanceSbeMsg::DepthSnapshot),
        depth_diff::TEMPLATE_ID => depth_diff::decode(msg, &hdr).map(BinanceSbeMsg::DepthDiff),
        trade::TEMPLATE_ID => trade::decode(msg, &hdr).map(BinanceSbeMsg::Trades),
        other => Err(DecodeError::UnknownTemplate(other)),
    }
}
