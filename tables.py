"""DDL for the new tables + insert column orders.

All DDL is CREATE TABLE IF NOT EXISTS and follows the conventions of the existing
crypto.* tables (LowCardinality, Decimal(38,18), DateTime64(3,'UTC'),
ReplacingMergeTree, partitioning, Map extra), plus per-column compression CODECs tuned
for tick data: monotonic ids/timestamps use DoubleDelta+ZSTD, Decimals use ZSTD.
It NEVER touches the 4 pre-existing tables (symbol_info, ohlcv, funding_rate,
open_interest) — those are CREATEd-if-missing only and we populate, never ALTER, them.
"""
from __future__ import annotations

# ---- insert column orders (used by both backfill and live) ----------------
TRADES_COLS = [
    "exchange", "market_type", "symbol", "trade_id", "price", "qty", "quote_qty",
    "transact_ts", "is_buyer_maker", "source_ts", "ingested_at", "extra",
]
BOOK_DEPTH_COLS = [
    "exchange", "market_type", "symbol", "snapshot_ts", "percentage", "depth",
    "notional", "source_ts", "ingested_at", "extra",
]
BOOK_L2_COLS = [
    "exchange", "market_type", "symbol", "snapshot_ts", "side", "level", "price",
    "qty", "source_ts", "ingested_at", "extra",
]
METRICS_COLS = [
    "exchange", "market_type", "symbol", "ts", "sum_open_interest",
    "sum_open_interest_value", "count_toptrader_long_short_ratio",
    "sum_toptrader_long_short_ratio", "count_long_short_ratio",
    "sum_taker_long_short_vol_ratio", "source_ts", "ingested_at", "extra",
]
INGEST_STATE_COLS = [
    "dataset", "symbol", "date", "status", "rows", "bytes", "sha256", "updated_at",
]
# ---- insert column orders for the PRE-EXISTING tables we now populate ----
OHLCV_COLS = [
    "exchange", "market_type", "symbol", "interval", "ts_open", "open", "high", "low",
    "close", "volume_base", "volume_quote", "trades", "taker_buy_base", "taker_buy_quote",
    "is_final", "source_ts", "ingested_at", "extra",
]
OPEN_INTEREST_COLS = [
    "exchange", "market_type", "symbol", "ts", "open_interest_qty", "open_interest_value",
    "oi_currency", "mark_price", "source_ts", "ingested_at", "extra",
]
FUNDING_RATE_COLS = [
    "exchange", "market_type", "symbol", "funding_ts", "funding_rate", "interval_hours",
    "mark_price", "index_price", "next_funding_ts", "predicted_rate", "source_ts",
    "ingested_at", "extra",
]
SYMBOL_INFO_COLS = [
    "exchange", "market_type", "symbol", "base_asset", "quote_asset", "contract_type",
    "settlement_asset", "margin_asset", "is_inverse", "multiplier", "price_precision",
    "qty_precision", "tick_size", "step_size", "min_qty", "max_qty", "min_notional",
    "max_notional", "status", "onboard_ts", "expire_ts", "ws_symbol", "api_symbol",
    "extra", "updated_at",
]

# ---- DDL -------------------------------------------------------------------
DDL: dict[str, str] = {
    "trades": """
CREATE TABLE IF NOT EXISTS crypto.trades
(
    exchange       LowCardinality(String),
    market_type    Enum8('spot'=1,'um'=2,'cm'=3),
    symbol         LowCardinality(String),
    trade_id       UInt64            CODEC(DoubleDelta, ZSTD(1)),
    price          Decimal(38,18)    CODEC(ZSTD(1)),
    qty            Decimal(38,18)    CODEC(ZSTD(1)),
    quote_qty      Nullable(Decimal(38,18)) CODEC(ZSTD(1)),
    transact_ts    DateTime64(3,'UTC') CODEC(DoubleDelta, ZSTD(1)),
    is_buyer_maker UInt8             CODEC(ZSTD(1)),
    source_ts      DateTime64(3,'UTC') CODEC(DoubleDelta, ZSTD(1)),
    ingested_at    DateTime64(3,'UTC') CODEC(DoubleDelta, ZSTD(1)),
    extra          Map(String,String)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(transact_ts)
ORDER BY (exchange, market_type, symbol, transact_ts, trade_id)
SETTINGS index_granularity = 8192
""",
    "book_depth": """
CREATE TABLE IF NOT EXISTS crypto.book_depth
(
    exchange    LowCardinality(String),
    market_type Enum8('spot'=1,'um'=2,'cm'=3),
    symbol      LowCardinality(String),
    snapshot_ts DateTime64(3,'UTC') CODEC(DoubleDelta, ZSTD(1)),
    percentage  Int16             CODEC(ZSTD(1)),
    depth       Decimal(38,18)    CODEC(ZSTD(1)),
    notional    Decimal(38,18)    CODEC(ZSTD(1)),
    source_ts   DateTime64(3,'UTC') CODEC(DoubleDelta, ZSTD(1)),
    ingested_at DateTime64(3,'UTC') CODEC(DoubleDelta, ZSTD(1)),
    extra       Map(String,String)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (exchange, market_type, symbol, snapshot_ts, percentage)
SETTINGS index_granularity = 8192
""",
    "book_snapshot_l2": """
CREATE TABLE IF NOT EXISTS crypto.book_snapshot_l2
(
    exchange    LowCardinality(String),
    market_type Enum8('spot'=1,'um'=2,'cm'=3),
    symbol      LowCardinality(String),
    snapshot_ts DateTime64(3,'UTC') CODEC(DoubleDelta, ZSTD(1)),
    side        Enum8('bid'=1,'ask'=2),
    level       UInt16            CODEC(ZSTD(1)),
    price       Decimal(38,18)    CODEC(ZSTD(1)),
    qty         Decimal(38,18)    CODEC(ZSTD(1)),
    source_ts   DateTime64(3,'UTC') CODEC(DoubleDelta, ZSTD(1)),
    ingested_at DateTime64(3,'UTC') CODEC(DoubleDelta, ZSTD(1)),
    extra       Map(String,String)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(snapshot_ts)
ORDER BY (exchange, market_type, symbol, snapshot_ts, side, level)
SETTINGS index_granularity = 8192
""",
    "futures_metrics": """
CREATE TABLE IF NOT EXISTS crypto.futures_metrics
(
    exchange                         LowCardinality(String),
    market_type                      Enum8('spot'=1,'um'=2,'cm'=3),
    symbol                           LowCardinality(String),
    ts                               DateTime64(3,'UTC') CODEC(DoubleDelta, ZSTD(1)),
    sum_open_interest                Nullable(Decimal(38,18)) CODEC(ZSTD(1)),
    sum_open_interest_value          Nullable(Decimal(38,18)) CODEC(ZSTD(1)),
    count_toptrader_long_short_ratio Nullable(Decimal(18,8)) CODEC(ZSTD(1)),
    sum_toptrader_long_short_ratio   Nullable(Decimal(18,8)) CODEC(ZSTD(1)),
    count_long_short_ratio           Nullable(Decimal(18,8)) CODEC(ZSTD(1)),
    sum_taker_long_short_vol_ratio   Nullable(Decimal(18,8)) CODEC(ZSTD(1)),
    source_ts                        DateTime64(3,'UTC') CODEC(DoubleDelta, ZSTD(1)),
    ingested_at                      DateTime64(3,'UTC') CODEC(DoubleDelta, ZSTD(1)),
    extra                            Map(String,String)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (exchange, market_type, symbol, ts)
SETTINGS index_granularity = 8192
""",
    "ingest_state": """
CREATE TABLE IF NOT EXISTS crypto.ingest_state
(
    dataset    LowCardinality(String),
    symbol     LowCardinality(String),
    date       Date,
    status     Enum8('done'=1,'empty'=2,'error'=3),
    rows       UInt64,
    bytes      UInt64,
    sha256     String,
    updated_at DateTime64(3,'UTC') CODEC(DoubleDelta, ZSTD(1))
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (dataset, symbol, date)
SETTINGS index_granularity = 8192
""",
}
