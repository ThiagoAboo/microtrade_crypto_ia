CREATE DATABASE IF NOT EXISTS microtrade;

CREATE TABLE IF NOT EXISTS microtrade.ticks
(
    timestamp DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(3)),
    exchange_ts DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(3)),
    received_ts DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(3)),
    processed_ts DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(3)),
    symbol LowCardinality(String),
    price Float64 CODEC(Gorilla, ZSTD(3)),
    quantity Float64 CODEC(Gorilla, ZSTD(3)),
    side Enum8('buy' = 1, 'sell' = -1, 'unknown' = 0),
    trade_id String,
    source LowCardinality(String) DEFAULT 'unknown',
    source_exchange LowCardinality(String) DEFAULT 'binance_spot',
    sequence UInt64 DEFAULT 0,
    ingest_latency_ms Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (symbol, timestamp, trade_id)
TTL toDateTime(timestamp) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS microtrade.orderbook_snapshots
(
    timestamp DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(3)),
    exchange_ts DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(3)),
    received_ts DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(3)),
    processed_ts DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(3)),
    symbol LowCardinality(String),
    best_bid Float64 CODEC(Gorilla, ZSTD(3)),
    best_ask Float64 CODEC(Gorilla, ZSTD(3)),
    bid_levels_json String CODEC(ZSTD(3)),
    ask_levels_json String CODEC(ZSTD(3)),
    spread Float64 CODEC(Gorilla, ZSTD(3)),
    source LowCardinality(String) DEFAULT 'unknown',
    source_exchange LowCardinality(String) DEFAULT 'binance_spot',
    levels UInt8 DEFAULT 10,
    last_update_id UInt64 DEFAULT 0,
    first_update_id UInt64 DEFAULT 0,
    final_update_id UInt64 DEFAULT 0,
    sync_state LowCardinality(String) DEFAULT 'unknown',
    sequence_integrity LowCardinality(String) DEFAULT 'unknown',
    ingest_latency_ms Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (symbol, timestamp)
TTL toDateTime(timestamp) + INTERVAL 15 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS microtrade.system_events
(
    timestamp DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(3)),
    event_id UUID,
    event_type LowCardinality(String),
    source LowCardinality(String),
    symbol Nullable(String),
    severity Enum8('debug' = 1, 'info' = 2, 'warning' = 3, 'error' = 4, 'critical' = 5),
    message String CODEC(ZSTD(3)),
    payload_json String CODEC(ZSTD(3)),
    latency_ms Nullable(Float64) CODEC(Gorilla, ZSTD(3)),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, event_type, source)
TTL toDateTime(timestamp) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192;
