//! [DATA PLANE] Hot path: Binance SBE message -> symbol slot -> FeedId -> MdEvent(s).
//! No allocation, no strings, no locks. Lookups: one hash probe + one array index.
//! Text frames are request responses (`{"result":null,"id":N}` or an error with the id),
//! reported as acks to the subscription state.
#![deny(clippy::disallowed_methods, clippy::disallowed_macros)]

use super::sbe::dispatch::{BinanceSbeMsg, decode};
use super::sbe::codec::{Group, levels};
use crate::transport::ws::reader::MessageKind;
use crate::data_plane::decimal::parse_u64;
use crate::data_plane::json_scan::raw_field;
use crate::control_plane::configs_struct::{DATA_TYPE_SLOTS, MarketDataType};
use crate::control_plane::identity::FeedId;
use crate::control_plane::startup::resolver::ConnectionPlan;
use crate::data_plane::events::{EventKind, MdEvent, flags};
use crate::data_plane::route_table::RouteTable;
use crate::venues::{Ack, EmitCtx, VenueHandler};

const NO_FEED: u16 = u16::MAX;
const SLOT_QUOTE: usize = MarketDataType::Quote.slot();
const SLOT_TRADE: usize = MarketDataType::Trade.slot();
const SLOT_SNAPSHOT: usize = MarketDataType::OrderBookSnapshot { depth: 0 }.slot();
const SLOT_DELTA: usize = MarketDataType::OrderBookDelta.slot();

pub struct BinanceSpotHandler {
    symbols: RouteTable,
    /// `routes[symbol_slot][data_type_slot]` -> feed id or NO_FEED.
    routes: Box<[[u16; DATA_TYPE_SLOTS]]>,
}

impl BinanceSpotHandler {
    #[allow(clippy::disallowed_methods, clippy::disallowed_macros)] // control plane: runs once at startup
    pub fn new(plan: &ConnectionPlan) -> anyhow::Result<Self> {
        let symbols = RouteTable::new(&plan.route_keys).map_err(|e| anyhow::anyhow!("{}: {e:?}", plan.name))?;
        let routes = plan
            .routes
            .iter()
            .map(|r| r.map(|f| f.map_or(NO_FEED, |f| f.0)))
            .collect();
        Ok(Self { symbols, routes })
    }

    #[inline(always)]
    fn route(&self, symbol: &[u8], data_slot: usize, ctx: &mut EmitCtx<'_>) -> Option<FeedId> {
        let sym = self.symbols.lookup(symbol)?;
        // sym < routes.len() by construction (table built from the same plan).
        let raw = self.routes.get(sym)?[data_slot];
        let feed = FeedId(raw);
        if raw == NO_FEED || !ctx.active.contains(feed) {
            return None;
        }
        Some(feed)
    }
}

impl VenueHandler for BinanceSpotHandler {
    #[inline]
    fn on_message(&mut self, kind: MessageKind, payload: &[u8], ctx: &mut EmitCtx<'_>) {
        if kind == MessageKind::Text {
            on_response(payload, ctx);
            return;
        }
        let Ok(msg) = decode(payload) else { return };
        let t = ctx.recv_tsc;

        match msg {
            BinanceSbeMsg::BestBidAsk(q) => {
                let Some(feed) = self.route(q.symbol, SLOT_QUOTE, ctx) else { return };
                let mut e = MdEvent::new(feed, EventKind::Quote, t);
                e.exch_ts_us = q.event_time_us;
                e.seq = q.book_update_id;
                e.px = q.bid_price;
                e.qty = q.bid_qty;
                e.aux0 = q.ask_price;
                e.aux1 = q.ask_qty;
                e.price_exp = q.price_exponent;
                e.qty_exp = q.qty_exponent;
                ctx.out.emit(e);
            }
            BinanceSbeMsg::Trades(tr) => {
                let Some(feed) = self.route(tr.symbol, SLOT_TRADE, ctx) else { return };
                for x in tr.iter() {
                    let mut e = MdEvent::new(feed, EventKind::Trade, t);
                    e.exch_ts_us = tr.event_time_us;
                    e.seq = x.id;
                    e.px = x.price;
                    e.qty = x.qty;
                    e.aux0 = tr.transact_time_us;
                    e.flags = if x.is_buyer_maker { flags::BUYER_MAKER } else { 0 };
                    e.price_exp = tr.price_exponent;
                    e.qty_exp = tr.qty_exponent;
                    ctx.out.emit(e);
                }
            }
            BinanceSbeMsg::DepthSnapshot(d) => {
                let Some(feed) = self.route(d.symbol, SLOT_SNAPSHOT, ctx) else { return };
                let mut base = MdEvent::new(feed, EventKind::BookSnapshot, t);
                base.exch_ts_us = d.event_time_us;
                base.seq = d.book_update_id;
                base.price_exp = d.price_exponent;
                base.qty_exp = d.qty_exponent;
                emit_book(base, d.bids, d.asks, ctx);
            }
            BinanceSbeMsg::DepthDiff(d) => {
                let Some(feed) = self.route(d.symbol, SLOT_DELTA, ctx) else { return };
                let mut base = MdEvent::new(feed, EventKind::BookDelta, t);
                base.exch_ts_us = d.event_time_us;
                base.seq = d.last_book_update_id;
                base.aux0 = d.first_book_update_id;
                base.price_exp = d.price_exponent;
                base.qty_exp = d.qty_exponent;
                emit_book(base, d.bids, d.asks, ctx);
            }
        }
    }
}

/// `{"result":null,"id":7}` -> RequestOk(7); anything with an error and an id -> RequestFailed.
#[cold]
#[inline(never)]
fn on_response(payload: &[u8], ctx: &mut EmitCtx<'_>) {
    let id = raw_field(payload, b"id").and_then(parse_u64);
    let is_error = raw_field(payload, b"result").is_none();
    match (id, is_error) {
        (Some(id), false) => ctx.control.ack(Ack::RequestOk(id)),
        (Some(id), true) => ctx.control.ack(Ack::RequestFailed(id)),
        (None, _) => ctx.control.ack(Ack::Error),
    }
}

/// One event per level, bids then asks, framed by FIRST/LAST (EMPTY when there are no levels).
/// All-or-nothing: if the ring cannot take every level, the whole book is rewound.
#[inline(always)]
fn emit_book(base: MdEvent, bids: Group<'_>, asks: Group<'_>, ctx: &mut EmitCtx<'_>) {
    let total = bids.len() + asks.len();
    if total == 0 {
        let mut e = base;
        e.flags = flags::EMPTY | flags::FIRST | flags::LAST;
        ctx.out.emit(e);
        return;
    }
    let mark = ctx.out.mark();
    let mut complete = true;
    let mut n = 0usize;
    for (side, group) in [(flags::BID, bids), (flags::ASK, asks)] {
        for (px, qty) in levels(group) {
            let mut e = base;
            e.px = px;
            e.qty = qty;
            e.flags = side | if n == 0 { flags::FIRST } else { 0 } | if n + 1 == total { flags::LAST } else { 0 };
            complete &= ctx.out.emit(e);
            n += 1;
        }
    }
    if !complete {
        ctx.out.rewind(mark);
    }
}
