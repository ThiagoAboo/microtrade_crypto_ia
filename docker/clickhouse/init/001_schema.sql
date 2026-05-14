CREATE DATABASE IF NOT EXISTS microtrade;

CREATE TABLE IF NOT EXISTS microtrade.ticks
(
    timestamp DateTime64(3, 'UTC') CODEC(ZSTD(3)),
    symbol LowCardinality(String) CODEC(ZSTD(3)),
    price Float64 CODEC(ZSTD(3)),
    quantity Float64 CODEC(ZSTD(3)),
    side LowCardinality(String) CODEC(ZSTD(3)),
    trade_id String CODEC(ZSTD(3)),
    ingested_at DateTime DEFAULT now() CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (symbol, timestamp, trade_id)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS microtrade.orderbook_snapshots
(
    timestamp DateTime64(3, 'UTC') CODEC(ZSTD(3)),
    symbol LowCardinality(String) CODEC(ZSTD(3)),
    best_bid Float64 CODEC(ZSTD(3)),
    best_ask Float64 CODEC(ZSTD(3)),
    bid_levels_json String CODEC(ZSTD(3)),
    ask_levels_json String CODEC(ZSTD(3)),
    spread Float64 CODEC(ZSTD(3)),
    ingested_at DateTime DEFAULT now() CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (symbol, timestamp)
TTL toDateTime(timestamp) + INTERVAL 15 DAY;

CREATE TABLE IF NOT EXISTS microtrade.features
(
    timestamp DateTime64(3, 'UTC') CODEC(ZSTD(3)),
    symbol LowCardinality(String) CODEC(ZSTD(3)),
    order_flow_imbalance Float64 CODEC(ZSTD(3)),
    microprice Float64 CODEC(ZSTD(3)),
    volatility_score Float64 CODEC(ZSTD(3)),
    liquidity_shift Float64 CODEC(ZSTD(3)),
    queue_pressure Float64 CODEC(ZSTD(3)),
    ingested_at DateTime DEFAULT now() CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (symbol, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS microtrade.signals
(
    timestamp DateTime64(3, 'UTC') CODEC(ZSTD(3)),
    symbol LowCardinality(String) CODEC(ZSTD(3)),
    direction LowCardinality(String) CODEC(ZSTD(3)),
    confidence Float64 CODEC(ZSTD(3)),
    expected_move Float64 CODEC(ZSTD(3)),
    ingested_at DateTime DEFAULT now() CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (symbol, timestamp)
TTL toDateTime(timestamp) + INTERVAL 90 DAY;

CREATE TABLE IF NOT EXISTS microtrade.orders
(
    created_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(ZSTD(3)),
    order_id String CODEC(ZSTD(3)),
    symbol LowCardinality(String) CODEC(ZSTD(3)),
    side LowCardinality(String) CODEC(ZSTD(3)),
    quantity Float64 CODEC(ZSTD(3)),
    price Nullable(Float64) CODEC(ZSTD(3)),
    status LowCardinality(String) CODEC(ZSTD(3)),
    exchange_order_id Nullable(String) CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (symbol, created_at, order_id)
TTL toDateTime(created_at) + INTERVAL 180 DAY;

CREATE TABLE IF NOT EXISTS microtrade.fills
(
    timestamp DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(ZSTD(3)),
    fill_id String CODEC(ZSTD(3)),
    order_id String CODEC(ZSTD(3)),
    symbol LowCardinality(String) CODEC(ZSTD(3)),
    fill_price Float64 CODEC(ZSTD(3)),
    fill_quantity Float64 CODEC(ZSTD(3)),
    fees Float64 CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (symbol, timestamp, order_id, fill_id)
TTL toDateTime(timestamp) + INTERVAL 180 DAY;

CREATE TABLE IF NOT EXISTS microtrade.positions
(
    opened_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(ZSTD(3)),
    closed_at Nullable(DateTime64(3, 'UTC')) CODEC(ZSTD(3)),
    position_id String CODEC(ZSTD(3)),
    symbol LowCardinality(String) CODEC(ZSTD(3)),
    entry_price Float64 CODEC(ZSTD(3)),
    exit_price Nullable(Float64) CODEC(ZSTD(3)),
    pnl Float64 CODEC(ZSTD(3)),
    duration UInt32 CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(opened_at)
ORDER BY (symbol, opened_at, position_id)
TTL toDateTime(opened_at) + INTERVAL 365 DAY;

