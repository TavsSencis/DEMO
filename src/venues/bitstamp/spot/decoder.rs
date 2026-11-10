//! [DATA PLANE] Hot path: Bitstamp JSON message -> channel -> route slot -> (market, instrument) -> MdEvent(s).
//! No allocation: values are read in place with the shape-aware scanner and decimal strings
//! are converted straight to fixed-point mantissas at the venue instrument's scale (daily reference data). Book messages are
//! parsed and emitted in one pass; a malformed level rewinds the unpublished events, so a
//! half-emitted book is never published.
#![deny(clippy::disallowed_methods, clippy::disallowed_macros)]

use crate::transport::ws::reader::MessageKind;
use crate::data_plane::decimal::{parse_fixed, parse_u64};
use crate::data_plane::json_scan::{StrPairs, container_field, raw_field, str_field};
use crate::control_plane::configs_struct::MarketDataType;
use crate::control_plane::identity::{InstrumentId, MarketId, StreamId};
use crate::control_plane::startup::resolver::{ConnectionPlan, StartupPlan};
use crate::data_plane::events::{BookEmitter, EventKind, MdEvent, flags};
use crate::data_plane::route_table::RouteTable;
use crate::venues::{Ack, EmitCtx, VenueHandler};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Trade,
    Snapshot,
    Delta,
}

/// Everything the decoder needs for one channel, indexed by route slot.
#[derive(Clone, Copy)]
struct Route {
    market: MarketId,
    instrument: InstrumentId,
    /// The channel's subscription; used only on the cold ack path.
    stream: StreamId,
    kind: Kind,
    price_exp: i8,
    qty_exp: i8,
}

pub struct BitstampSpotHandler {
    channels: RouteTable,
    routes: Box<[Route]>,
}

impl BitstampSpotHandler {
    #[allow(clippy::disallowed_methods, clippy::disallowed_macros)] // control plane: runs once at startup
    pub fn new(plan: &StartupPlan, conn: &ConnectionPlan) -> anyhow::Result<Self> {
        let channels = RouteTable::new(&conn.route_keys).map_err(|e| anyhow::anyhow!("{}: {e:?}", conn.name))?;
        let mut routes = Vec::with_capacity(conn.routes.len());
        for (slot, target) in conn.routes.iter().enumerate() {
            // Each Bitstamp channel carries exactly one data type.
            let stream = target
                .streams
                .iter()
                .flatten()
                .next()
                .copied()
                .ok_or_else(|| anyhow::anyhow!("{}: channel {} has no stream", conn.name, conn.route_keys[slot]))?;
            let spec = plan.stream(stream);
            let kind = match spec.data_type {
                MarketDataType::Trade => Kind::Trade,
                MarketDataType::OrderBookSnapshot { .. } => Kind::Snapshot,
                MarketDataType::OrderBookDelta => Kind::Delta,
                MarketDataType::Quote => anyhow::bail!("{}: Bitstamp has no quote channel", conn.name),
            };
            routes.push(Route {
                market: target.market,
                instrument: target.instrument,
                stream,
                kind,
                price_exp: target.price_exp,
                qty_exp: target.qty_exp,
            });
        }
        Ok(Self { channels, routes: routes.into_boxed_slice() })
    }
}

impl VenueHandler for BitstampSpotHandler {
    #[inline]
    fn on_message(&mut self, kind: MessageKind, payload: &[u8], ctx: &mut EmitCtx<'_>) {
        if kind != MessageKind::Text {
            return;
        }
        // Locate `data` first and look for the envelope keys outside it, so large book
        // payloads are not rescanned for `event`/`channel`.
        let data = container_field(payload, b"data");
        let (head, tail) = match data {
            Some(d) => {
                let start = d.as_ptr() as usize - payload.as_ptr() as usize;
                (&payload[..start], &payload[start + d.len()..])
            }
            None => (payload, &payload[..0]),
        };
        let field = |key: &[u8]| str_field(head, key).or_else(|| str_field(tail, key));
        let Some(event) = field(b"event") else { return };
        let channel = field(b"channel").unwrap_or(b"");

        if event.starts_with(b"bts:") {
            self.on_control(event, channel, ctx);
            return;
        }

        let Some(slot) = self.channels.lookup(channel) else { return };
        let r = self.routes[slot];
        let Some(data) = data else { return };
        // A malformed message emits nothing.
        match r.kind {
            Kind::Trade => emit_trade(r, data, ctx),
            Kind::Snapshot => emit_book(r, EventKind::BookSnapshot, data, ctx),
            Kind::Delta => emit_book(r, EventKind::BookDelta, data, ctx),
        }
    }
}

impl BitstampSpotHandler {
    /// `bts:*` envelope events. Cold path.
    #[cold]
    #[inline(never)]
    fn on_control(&self, event: &[u8], channel: &[u8], ctx: &mut EmitCtx<'_>) {
        let stream = || self.channels.lookup(channel).map(|s| self.routes[s].stream);
        match event {
            b"bts:subscription_succeeded" => {
                if let Some(f) = stream() {
                    ctx.control.ack(Ack::Subscribed(f));
                }
            }
            b"bts:request_reconnect" => ctx.control.reconnect = true,
            b"bts:error" => ctx.control.ack(Ack::Error),
            _ => {} // bts:heartbeat replies and future events
        }
    }
}

/// `live_trades_*`: one trade per message. type 0 = buy taker, 1 = sell taker (buyer is maker).
#[inline(always)]
fn emit_trade(r: Route, data: &[u8], ctx: &mut EmitCtx<'_>) {
    let (Some(px), Some(qty), Some(id), Some(ts)) = (
        str_field(data, b"price_str").and_then(|s| parse_fixed(s, r.price_exp)),
        str_field(data, b"amount_str").and_then(|s| parse_fixed(s, r.qty_exp)),
        raw_field(data, b"id").and_then(parse_u64),
        str_field(data, b"microtimestamp").and_then(parse_u64),
    ) else {
        return;
    };
    let mut e = MdEvent::new(r.market, r.instrument, EventKind::Trade, ctx.recv_tsc);
    e.exch_ts_us = ts as i64;
    e.seq = id as i64;
    e.px = px;
    e.qty = qty;
    e.aux0 = ts as i64;
    e.flags = if raw_field(data, b"type") == Some(b"1") { flags::BUYER_MAKER } else { 0 };
    e.price_exp = r.price_exp;
    e.qty_exp = r.qty_exp;
    ctx.out.emit(e);
}

/// `order_book_*` (top-100 snapshot) and `diff_order_book_*` (changed levels; amount 0 =
/// remove). seq = microtimestamp, the only sequencing Bitstamp provides for books.
#[inline(always)]
fn emit_book(r: Route, kind: EventKind, data: &[u8], ctx: &mut EmitCtx<'_>) {
    let (Some(bids), Some(asks), Some(ts)) = (
        container_field(data, b"bids"),
        container_field(data, b"asks"),
        str_field(data, b"microtimestamp").and_then(parse_u64),
    ) else {
        return;
    };
    let mut base = MdEvent::new(r.market, r.instrument, kind, ctx.recv_tsc);
    base.exch_ts_us = ts as i64;
    base.seq = ts as i64;
    base.price_exp = r.price_exp;
    base.qty_exp = r.qty_exp;
    let mut book = BookEmitter::new(ctx.out, base);
    for (side_flag, side) in [(flags::BID, bids), (flags::ASK, asks)] {
        for (p, q) in StrPairs::new(side) {
            let (Some(px), Some(qty)) = (parse_fixed(p, r.price_exp), parse_fixed(q, r.qty_exp)) else {
                // Malformed level (e.g. more decimals than the reference data): drop the book.
                book.abort(ctx.out);
                return;
            };
            book.level(ctx.out, side_flag, px, qty);
        }
    }
    book.finish(ctx.out);
}
