from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from core.config import ClickHouseSettings, RetrySettings
from core.logging import get_logger
from core.retry import RetryPolicy, retry_async
from market_data.models import NormalizedTrade, ReducedOrderBookSnapshot

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client


class ClickHouseMarketDataWriter:
    def __init__(self, settings: ClickHouseSettings, retry_settings: RetrySettings) -> None:
        self._settings = settings
        self._retry_policy = RetryPolicy(
            max_attempts=retry_settings.max_attempts,
            base_delay_seconds=retry_settings.base_delay_seconds,
            max_delay_seconds=retry_settings.max_delay_seconds,
            backoff_multiplier=retry_settings.backoff_multiplier,
        )
        self._client: Client | None = None
        self._lock = asyncio.Lock()
        self._logger = get_logger(__name__)

    async def ensure_schema(self) -> None:
        async with self._lock:
            await retry_async(
                lambda: asyncio.to_thread(self._ensure_schema_once),
                self._retry_policy,
                operation_name="clickhouse.market_data.ensure_schema",
                logger=self._logger,
            )

    async def insert_trades(self, trades: list[NormalizedTrade]) -> None:
        if not trades:
            return
        rows = [_trade_to_row(trade) for trade in trades]
        columns = list(rows[0].keys())
        async with self._lock:
            await retry_async(
                lambda: asyncio.to_thread(
                    self._get_client().insert,
                    "ticks",
                    [tuple(row[column] for column in columns) for row in rows],
                    column_names=columns,
                ),
                self._retry_policy,
                operation_name="clickhouse.market_data.insert_trades",
                logger=self._logger,
            )

    async def insert_orderbook_snapshots(self, snapshots: list[ReducedOrderBookSnapshot]) -> None:
        if not snapshots:
            return
        rows = [_orderbook_to_row(snapshot) for snapshot in snapshots]
        columns = list(rows[0].keys())
        async with self._lock:
            await retry_async(
                lambda: asyncio.to_thread(
                    self._get_client().insert,
                    "orderbook_snapshots",
                    [tuple(row[column] for column in columns) for row in rows],
                    column_names=columns,
                ),
                self._retry_policy,
                operation_name="clickhouse.market_data.insert_orderbook_snapshots",
                logger=self._logger,
            )

    async def close(self) -> None:
        async with self._lock:
            if self._client is not None:
                await asyncio.to_thread(self._client.close)
                self._client = None

    def _ensure_schema_once(self) -> None:
        client = self._get_client()
        for statement in [*_PHASE2_CREATE_TABLE_STATEMENTS, *_PHASE2_SCHEMA_STATEMENTS]:
            client.command(statement)

    def _get_client(self) -> Client:
        if self._client is None:
            import clickhouse_connect

            try:
                self._client = self._connect_client(clickhouse_connect, self._settings.database)
            except Exception:
                bootstrap_client = self._connect_client(clickhouse_connect, "default")
                try:
                    bootstrap_client.command(
                        f"CREATE DATABASE IF NOT EXISTS {_quote_identifier(self._settings.database)}"
                    )
                finally:
                    bootstrap_client.close()
                self._client = self._connect_client(clickhouse_connect, self._settings.database)
        return self._client

    def _connect_client(self, clickhouse_connect: Any, database: str) -> Client:
        return clickhouse_connect.get_client(
            host=self._settings.host,
            port=self._settings.port,
            username=self._settings.username,
            password=self._settings.password,
            database=database,
            connect_timeout=self._settings.connect_timeout_seconds,
            send_receive_timeout=self._settings.send_receive_timeout_seconds,
            autogenerate_session_id=False,
        )


def _trade_to_row(trade: NormalizedTrade) -> dict[str, object]:
    return {
        "timestamp": _datetime_from_ms(trade.exchange_ts),
        "exchange_ts": _datetime_from_ms(trade.exchange_ts),
        "received_ts": _datetime_from_ms(trade.received_ts),
        "processed_ts": _datetime_from_ms(trade.processed_ts),
        "symbol": trade.symbol,
        "price": trade.price,
        "quantity": trade.quantity,
        "side": trade.side,
        "trade_id": trade.trade_id,
        "source": trade.source_exchange,
        "source_exchange": trade.source_exchange,
        "sequence": trade.sequence,
        "ingest_latency_ms": trade.ingest_latency_ms,
    }


def _orderbook_to_row(snapshot: ReducedOrderBookSnapshot) -> dict[str, object]:
    return {
        "timestamp": _datetime_from_ms(snapshot.exchange_ts),
        "exchange_ts": _datetime_from_ms(snapshot.exchange_ts),
        "received_ts": _datetime_from_ms(snapshot.received_ts),
        "processed_ts": _datetime_from_ms(snapshot.processed_ts),
        "symbol": snapshot.symbol,
        "best_bid": snapshot.best_bid,
        "best_ask": snapshot.best_ask,
        "bid_levels_json": json.dumps(snapshot.bid_levels, separators=(",", ":")),
        "ask_levels_json": json.dumps(snapshot.ask_levels, separators=(",", ":")),
        "spread": snapshot.spread,
        "source": snapshot.source_exchange,
        "source_exchange": snapshot.source_exchange,
        "levels": snapshot.levels,
        "last_update_id": snapshot.last_update_id,
        "first_update_id": snapshot.first_update_id,
        "final_update_id": snapshot.final_update_id,
        "sync_state": snapshot.sync_state.value,
        "sequence_integrity": snapshot.sequence_integrity.value,
        "ingest_latency_ms": snapshot.ingest_latency_ms,
    }


def _datetime_from_ms(timestamp_ms: int) -> datetime:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


_PHASE2_CREATE_TABLE_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS ticks
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
    SETTINGS index_granularity = 8192
    """,
    """
    CREATE TABLE IF NOT EXISTS orderbook_snapshots
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
    SETTINGS index_granularity = 8192
    """,
)


_PHASE2_SCHEMA_STATEMENTS = (
    "ALTER TABLE ticks ADD COLUMN IF NOT EXISTS exchange_ts DateTime64(3, 'UTC') "
    "CODEC(DoubleDelta, ZSTD(3)) AFTER timestamp",
    "ALTER TABLE ticks ADD COLUMN IF NOT EXISTS received_ts DateTime64(3, 'UTC') "
    "CODEC(DoubleDelta, ZSTD(3)) AFTER exchange_ts",
    "ALTER TABLE ticks ADD COLUMN IF NOT EXISTS processed_ts DateTime64(3, 'UTC') "
    "CODEC(DoubleDelta, ZSTD(3)) AFTER received_ts",
    "ALTER TABLE ticks ADD COLUMN IF NOT EXISTS source_exchange LowCardinality(String) "
    "DEFAULT 'binance_spot' AFTER source",
    "ALTER TABLE ticks ADD COLUMN IF NOT EXISTS sequence UInt64 DEFAULT 0 AFTER source_exchange",
    "ALTER TABLE ticks ADD COLUMN IF NOT EXISTS ingest_latency_ms Nullable(Float64) "
    "CODEC(Gorilla, ZSTD(3)) AFTER sequence",
    "ALTER TABLE orderbook_snapshots ADD COLUMN IF NOT EXISTS exchange_ts DateTime64(3, 'UTC') "
    "CODEC(DoubleDelta, ZSTD(3)) AFTER timestamp",
    "ALTER TABLE orderbook_snapshots ADD COLUMN IF NOT EXISTS received_ts DateTime64(3, 'UTC') "
    "CODEC(DoubleDelta, ZSTD(3)) AFTER exchange_ts",
    "ALTER TABLE orderbook_snapshots ADD COLUMN IF NOT EXISTS processed_ts DateTime64(3, 'UTC') "
    "CODEC(DoubleDelta, ZSTD(3)) AFTER received_ts",
    "ALTER TABLE orderbook_snapshots ADD COLUMN IF NOT EXISTS source_exchange LowCardinality(String) "
    "DEFAULT 'binance_spot' AFTER source",
    "ALTER TABLE orderbook_snapshots ADD COLUMN IF NOT EXISTS levels UInt8 DEFAULT 10 AFTER source_exchange",
    "ALTER TABLE orderbook_snapshots ADD COLUMN IF NOT EXISTS last_update_id UInt64 DEFAULT 0 AFTER levels",
    "ALTER TABLE orderbook_snapshots ADD COLUMN IF NOT EXISTS first_update_id UInt64 DEFAULT 0 "
    "AFTER last_update_id",
    "ALTER TABLE orderbook_snapshots ADD COLUMN IF NOT EXISTS final_update_id UInt64 DEFAULT 0 "
    "AFTER first_update_id",
    "ALTER TABLE orderbook_snapshots ADD COLUMN IF NOT EXISTS sync_state LowCardinality(String) "
    "DEFAULT 'unknown' AFTER final_update_id",
    "ALTER TABLE orderbook_snapshots ADD COLUMN IF NOT EXISTS sequence_integrity LowCardinality(String) "
    "DEFAULT 'unknown' AFTER sync_state",
    "ALTER TABLE orderbook_snapshots ADD COLUMN IF NOT EXISTS ingest_latency_ms Nullable(Float64) "
    "CODEC(Gorilla, ZSTD(3)) AFTER sequence_integrity",
)
