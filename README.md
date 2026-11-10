# new_genesis

Low-latency, multi-venue crypto market-data connectivity in Rust.

In its current stage the project consists of 80 Rust source files (about 5,100 lines) in 18
directories, organised into five top-level modules: `control_plane`, `data_plane`, `venues`,
`transport` and `sys`.

Connects to several exchanges at once, each over its native protocol (binary SBE and JSON
WebSocket today), subscribes to any number of configured instruments per venue, decodes every
message into one fixed-size normalized event and hands it to a consumer through a lock-free
ring.

The code is organised as independent modules with narrow interfaces: transport, venue
protocol, routing and event delivery can each be replaced or optimised on its own, from
adding a venue to moving the transport from WebSocket to kernel-bypass networking, without
touching the rest of the pipeline.

> **Scope.** This is a connectivity demo: market data in, normalized events out. There is no
> order entry, no strategy, no order-book building and no persistence. The consumer is a
> placeholder that drains the rings.

## Design principles

- **Strict control-plane / data-plane split.** Everything that is not per-message work
  (configuration, reference data, resolution, connect, reconnect, subscriptions) runs on the
  cold path. The per-message path never allocates, never takes a lock, never sleeps and never
  touches strings or configuration.
- **Resolve once, index forever.** Venue names, symbols and channel names exist only at
  startup. They are turned into dense integer ids and pre-built tables; the hot path sees ids.
- **Zero-copy decoding.** Frames are parsed in place in one receive buffer. SBE fields are read
  from the buffer; JSON is scanned in place and decimal text is converted straight to
  fixed-point integers.
- **No blocking on the data path.** Sockets are non-blocking and busy-polled; the consumer
  spins. Waiting is allowed only on the control plane while no session is live (backoff,
  connect/TLS/WebSocket handshake).
- **One thread per connection, owning its socket.** Reads and writes (pong, heartbeat,
  subscribe) happen on the same thread; there is no shared writer.
- **Fail at startup, not on the hot path.** Every configuration and reference-data error is
  detected before the first connection is opened.
- **Rules are enforced by the compiler.** Data-plane modules deny allocation, formatting,
  `Instant::now`, `thread::sleep` and locks through Clippy (`clippy.toml`).

## Modularity and scaling

| Layer | Interface | Replaceable without touching |
|---|---|---|
| Transport (`transport/`) | bytes in, frames out | decoders, routing, events |
| Venue protocol (`venues/<exchange>/<market>/`) | `Venue` trait + decoder over a payload slice | transport, resolver, runtime |
| Routing and events (`data_plane/`) | `MarketId` + `InstrumentId` in a 64-byte `MdEvent` | venues, transport |
| Configuration (`config/genesis.toml`) | venues, instruments, data types | any code |

- **Multiple venues, multiple instruments at once.** Feeds are grouped into connections per
  venue and data type within each venue's limits. Every connection has its own thread, socket,
  decoder state and event ring; connections share no mutable state, so capacity grows with
  connections and cores.
- **New instrument:** one entry in the configuration and a reference-data refresh. No code.
- **New venue or market:** one module implementing `Venue` and a decoder, plus registry and
  startup-dispatch entries.
  Resolver, runtime and data plane are unchanged; dispatch is resolved at startup and
  monomorphized, so a venue adds no per-message indirection.
- **Transport upgrades.** Decoders consume payload slices and know nothing about sockets, so a
  faster transport (kernel bypass, UDP multicast feeds) replaces `transport/` only.
- **Production deployment** (CPU pinning, isolated cores, NUMA placement) is a separate layer
  over the same thread-per-connection model.

## Architecture

```
                              GENESIS
                                 │
            ┌────────────────────┴────────────────────┐
            │                                         │
   CONTROL PLANE / RUNTIME                       DATA PLANE
   cold: startup and per session                 hot: per message
            │                                         │
   config / refdata                              raw bytes
   registry                                      decoder
   resolver                                      routing
   startup                                       MdEvent
   lifecycle                                     SPSC ring
            │                                         │
            └────────────────────┬────────────────────┘
                                 ▼
                           BOOK ENGINE           planned
                                 ▼
                           STRATEGIES            planned
                                 ▼
                           RISK ENGINE           planned
                                 ▼
                            EXECUTION            planned
                                 ▼
                        VENUE ORDER ENTRY        planned
```

This repository implements the two planes up to the SPSC ring; the layers below are the target
design. The control plane resolves everything once (configuration, reference data, ids,
connections, subscription frames) and then supervises connection lifecycles; the data plane
only turns bytes into normalized events.

### From configuration to the first subscribe

1. **Configuration** (`config/genesis.toml`) names venues and canonical instruments:
   venue + `BTC/USDT` + `["quote", "trade"]`.
2. **Reference data** (`new_genesis refdata`, run once a day) reads each venue's REST API and
   writes a dated file mapping canonical instruments to native symbols and precision:
   the same `BTC/USDT` is `BTCUSDT` on one venue and `btcusdt` with prices at 10^-2 on another.
3. **Registry** assigns process-local dense ids to currencies and instruments.
4. **Resolver** produces an immutable `StartupPlan`: one `FeedSpec` per
   (venue, instrument, data type), grouped into connections per venue and data type, with
   stream names (`btcusdt@trade`, `live_trades_btcusdt`), route keys and exponents.
5. **Launch** builds every decoder and subscription state first (any error stops startup
   before a thread exists), then starts one thread per connection.
6. **Supervisor** connects (TCP, TLS, WebSocket upgrade), switches the socket to non-blocking
   and replays the pre-built SUBSCRIBE batches under the venue's rate limit. A feed becomes
   `Up` only when the venue acknowledges it (by request id or by channel, per venue);
   no ack within 10 s marks it `Failed` and it is retried.

### Identifiers

| Id | Assigned | Meaning |
|---|---|---|
| `VenueId` | in code (`registry/venue.rs`) | one exchange market, e.g. an exchange's spot market |
| `CurrencyId`, `InstrumentId` | at startup, from the config | canonical `BTC/USDT`, process-local |
| `MarketId` | at startup, first appearance in the config | (venue, instrument); tags every event, indexes per-market state |
| `StreamId` | at startup, order in the config | one subscribed stream (venue, instrument, data type); control plane only: subscribe, ack, `Up`/`Down`/`Failed` |

Numeric ids are never written to disk. Files refer to venues and instruments by name, so an id
can never point to the wrong instrument across restarts.

### The hot path, per message

| Step | Binary venue (SBE) | Text venue (JSON) |
|---|---|---|
| Frame | WebSocket header parsed in place | same |
| Decode | bounds-checked SBE view, exponent carried per message | in-place JSON scan, decimal text to fixed point at the reference-data scale |
| Route | `symbol` bytes ─► slot | `channel` bytes ─► slot |
| Emit | one `MdEvent` per quote/trade/level | same |
| Publish | one release store per message | same |

The route table is the single place where wire text becomes an id: a fixed 128-entry
open-addressing table built at startup, keys read with overlapping word loads, resident in L1.

`MdEvent` is one 64-byte cache line: receive TSC, exchange time, sequence, price and quantity
mantissas with their exponents, two auxiliary fields, market id, instrument id, kind and flags.
Events are tagged by market and instrument; the data type is the event kind. Stream status
events (`Up`, `Down`, `Failed`) carry the same market and instrument plus the data type, and
consumers use a market's data of a type only while that stream is `Up`. Each connection
has its own single-producer/single-consumer ring of 8192 events.

## Repository layout

```
config/genesis.toml                         node configuration: environment, refdata, runtime, feeds
src/
├── consumer.rs                             placeholder consumer: drains the event rings
├── control_plane
│   ├── configs_struct.rs                   configuration schema (NodeConfig, feeds, runtime)
│   ├── identity                            process-local ids
│   │   ├── currency_id.rs                  CurrencyId
│   │   ├── instrument_id.rs                InstrumentId: canonical instrument, cross-venue group
│   │   ├── market_id.rs                    MarketId: venue + instrument, tags every event
│   │   ├── mod.rs
│   │   ├── stream_id.rs                    StreamId: one subscription (control plane only)
│   │   └── venue_id.rs                     VenueId
│   ├── mod.rs
│   ├── node.rs                             Node: build, run, shutdown
│   ├── runtime                             while live, per session (cold)
│   │   ├── mod.rs
│   │   ├── stream_set.rs                   stream bitset (confirmed streams)
│   │   ├── subscriptions.rs                subscribe batches, acks, timeouts, retries, rate limit
│   │   └── supervisor.rs                   connection thread: connect, session, reconnect, backoff
│   └── startup                             once, before any connection
│       ├── config_file.rs                  TOML -> NodeConfig + Registry, all names checked
│       ├── launch.rs                       prepare every connection, then start threads
│       ├── mod.rs
│       ├── refdata                         daily reference data
│       │   ├── build.rs                    venue REST pairs -> canonical instruments
│       │   ├── daily_file.rs               dated JSON file: atomic save, load, age check
│       │   └── mod.rs
│       ├── registry                        what exists
│       │   ├── catalog.rs                  Registry: lookups by id and by name
│       │   ├── currency.rs                 currency definition
│       │   ├── error.rs                    RegistryError
│       │   ├── instrument.rs               instrument definition, canonical key "BTC/USDT"
│       │   ├── mod.rs
│       │   ├── validation.rs               consistency checks
│       │   ├── venue_instrument.rs         native symbol and exponents per venue
│       │   └── venue.rs                    supported venues
│       └── resolver                        config + registry -> plan
│           ├── error.rs                    PlanError
│           ├── mod.rs
│           ├── plan.rs                     StartupPlan, FeedSpec, ConnectionPlan
│           └── resolve.rs                  feeds, connection grouping, stream names, route keys
├── data_plane                              per message (hot)
│   ├── decimal.rs                          decimal text -> fixed-point integer
│   ├── events.rs                           MdEvent (64 B), EventOut, book framing
│   ├── json_scan.rs                        in-place JSON field scanning
│   ├── mod.rs
│   ├── route_table.rs                      symbol/channel bytes -> route slot
│   ├── session.rs                          per-message dispatch, heartbeat, housekeeping
│   └── spsc.rs                             single-producer/single-consumer ring
├── lib.rs
├── main.rs                                 `refdata` command or run the node
├── sys
│   ├── clock.rs                            rdtsc timestamps, TSC calibration
│   ├── memory.rs                           buffer prefaulting
│   ├── mod.rs
│   └── signal.rs                           SIGINT/SIGTERM -> stop flag
├── transport
│   ├── http.rs                             HTTPS GET (reference data only)
│   ├── mod.rs
│   ├── tcp.rs                              TCP connect with timeout
│   ├── tls.rs                              TLS client (OpenSSL), buffer settings
│   └── ws
│       ├── connection.rs                   upgraded WebSocket connection
│       ├── frame.rs                        RFC 6455 frame header parse/encode
│       ├── handshake.rs                    HTTP -> WebSocket upgrade
│       ├── mod.rs
│       ├── reader.rs                       busy-poll session loop, in-place frames, pings
│       └── writer.rs                       masked client frames (pong, heartbeat, subscribe)
└── venues
    ├── binance
    │   ├── credentials.rs                  Ed25519 API key
    │   ├── dispatch.rs                     dispatch by market
    │   ├── mod.rs
    │   └── spot
    │       ├── decoder.rs                  SBE message -> market -> MdEvent
    │       ├── mod.rs
    │       ├── refdata.rs                  instrument list parser (REST)
    │       ├── sbe                         SBE schema decoders (zero-copy views)
    │       │   ├── best_bidask.rs          best bid/ask
    │       │   ├── codec.rs                header, groups, bounds-checked loads
    │       │   ├── depth_20.rs             top-20 depth snapshot
    │       │   ├── depth_diff.rs           depth update
    │       │   ├── dispatch.rs             template id -> decoder
    │       │   ├── mod.rs
    │       │   └── trade.rs                trades
    │       └── venue.rs                    Venue impl: endpoints, streams, limits, connect
    ├── bitstamp
    │   ├── dispatch.rs                     dispatch by market
    │   ├── mod.rs
    │   └── spot
    │       ├── decoder.rs                  JSON message -> market -> MdEvent
    │       ├── mod.rs
    │       ├── refdata.rs                  instrument list parser (REST)
    │       └── venue.rs                    Venue impl: endpoints, channels, heartbeat
    ├── decode.rs                           decoder interface, acks, control signals
    ├── dispatch.rs                         startup dispatch by venue
    ├── mod.rs
    ├── venue_pair.rs                       venue pair in canonical terms
    └── venue.rs                            Venue trait
```

Files in `control_plane`, `data_plane`, `venues` and `sys` start with their plane:
`[CONTROL PLANE]` or `[DATA PLANE]`; in `transport`, connect code is control plane and the
frame reader/writer is data plane. Each venue lives
in `venues/<exchange>/<market>/` behind the `Venue` trait and is monomorphized; adding an
instrument on an existing venue needs no code.

## Build and run

Requirements: Rust 1.96 (edition 2024), OpenSSL, Linux x86_64. Venues that require
authentication read their API key from a file outside the repository
(`venues/<exchange>/credentials.rs`); public data needs no key.

```bash
cargo build --release
cargo run --release -- refdata      # fetch today's reference data
cargo run --release                 # run until Ctrl-C
cargo clippy --all-targets          # also enforces the data-plane rules
```

The node prints nothing; output goes to the event rings.

## Measurements

Single core, `rdtsc`, release build, development workstation; synthetic messages through the
real decoders.

| Path | p50 | p99 |
|---|---|---|
| SBE best bid/ask message: decode, route, event, publish | 8.0 ns | 9.5 ns |
| Route table lookup | 5.8 ns | 8.6 ns |
| JSON order book message, 20 levels per side | 3.0 µs | 5.5 µs |

Live runs: every configured feed acknowledged and receiving data, cross-venue prices
consistent, clean reconnect after a network drop.

## Not implemented

- Order entry, risk, strategy.
- Order-book building and snapshot/delta sequencing.
- Make-before-break reconnect (a short gap on every reconnect).
- CPU pinning, isolated cores, kernel-bypass networking.
- Logging and latency histograms.
