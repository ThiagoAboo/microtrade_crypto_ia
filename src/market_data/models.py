from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


SOURCE_EXCHANGE = "binance_spot"


class MarketDataEngineStatus(StrEnum):
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    DEGRADED = "degraded"
    STOPPING = "stopping"


class OrderBookSyncState(StrEnum):
    UNSYNCED = "unsynced"
    BUFFERING = "buffering"
    SYNCING = "syncing"
    SYNCED = "synced"
    RESYNCING = "resyncing"


class SequenceIntegrityState(StrEnum):
    UNKNOWN = "unknown"
    OK = "ok"
    GAP_DETECTED = "gap_detected"
    INVALID_SEQUENCE = "invalid_sequence"


class ApplyUpdateResult(StrEnum):
    APPLIED = "applied"
    IGNORED_OLD = "ignored_old"
    GAP_DETECTED = "gap_detected"
    INVALID_SEQUENCE = "invalid_sequence"
    BUFFERED = "buffered"


@dataclass(frozen=True, slots=True)
class NormalizedTrade:
    exchange_ts: int
    received_ts: int
    processed_ts: int
    symbol: str
    source_exchange: str
    sequence: int
    trade_id: str
    price: float
    quantity: float
    side: str
    is_buyer_maker: bool
    ingest_latency_ms: float

    def to_payload(self) -> dict[str, Any]:
        return {
            "exchange_ts": self.exchange_ts,
            "received_ts": self.received_ts,
            "processed_ts": self.processed_ts,
            "symbol": self.symbol,
            "source_exchange": self.source_exchange,
            "sequence": self.sequence,
            "trade_id": self.trade_id,
            "price": self.price,
            "quantity": self.quantity,
            "side": self.side,
            "is_buyer_maker": self.is_buyer_maker,
            "ingest_latency_ms": self.ingest_latency_ms,
        }


@dataclass(frozen=True, slots=True)
class DepthUpdate:
    exchange_ts: int
    received_ts: int
    processed_ts: int
    symbol: str
    source_exchange: str
    first_update_id: int
    final_update_id: int
    bids: tuple[tuple[str, str], ...]
    asks: tuple[tuple[str, str], ...]
    ingest_latency_ms: float


@dataclass(frozen=True, slots=True)
class RestDepthSnapshot:
    received_ts: int
    processed_ts: int
    symbol: str
    source_exchange: str
    last_update_id: int
    bids: tuple[tuple[str, str], ...]
    asks: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class ReducedOrderBookSnapshot:
    exchange_ts: int
    received_ts: int
    processed_ts: int
    symbol: str
    source_exchange: str
    sequence: int
    first_update_id: int
    final_update_id: int
    last_update_id: int
    best_bid: float
    best_ask: float
    spread: float
    bid_levels: tuple[tuple[str, str], ...]
    ask_levels: tuple[tuple[str, str], ...]
    levels: int
    sync_state: OrderBookSyncState
    sequence_integrity: SequenceIntegrityState
    ingest_latency_ms: float

    def to_payload(self) -> dict[str, Any]:
        return {
            "exchange_ts": self.exchange_ts,
            "received_ts": self.received_ts,
            "processed_ts": self.processed_ts,
            "symbol": self.symbol,
            "source_exchange": self.source_exchange,
            "sequence": self.sequence,
            "first_update_id": self.first_update_id,
            "final_update_id": self.final_update_id,
            "last_update_id": self.last_update_id,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "spread": self.spread,
            "bid_levels": list(self.bid_levels),
            "ask_levels": list(self.ask_levels),
            "levels": self.levels,
            "sync_state": self.sync_state.value,
            "sequence_integrity": self.sequence_integrity.value,
            "ingest_latency_ms": self.ingest_latency_ms,
        }


@dataclass(slots=True)
class SymbolMarketDataHealth:
    symbol: str
    connected: bool = False
    last_event_received_ts: int | None = None
    last_event_age_ms: int | None = None
    last_trade_ts: int | None = None
    last_depth_ts: int | None = None
    last_snapshot_ts: int | None = None
    reconnect_count: int = 0
    resync_count: int = 0
    orderbook_sync_state: OrderBookSyncState = OrderBookSyncState.UNSYNCED
    sequence_integrity_state: SequenceIntegrityState = SequenceIntegrityState.UNKNOWN
    queue_lag: int = 0
    tick_queue_lag: int = 0
    snapshot_queue_lag: int = 0
    dropped_trades: int = 0
    dropped_requeued_trades: int = 0
    dropped_snapshots: int = 0
    trades_received: int = 0
    depth_updates_received: int = 0
    snapshots_published: int = 0
    last_update_id: int | None = None
    last_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "connected": self.connected,
            "last_event_received_ts": self.last_event_received_ts,
            "last_event_age_ms": self.last_event_age_ms,
            "last_trade_ts": self.last_trade_ts,
            "last_depth_ts": self.last_depth_ts,
            "last_snapshot_ts": self.last_snapshot_ts,
            "reconnect_count": self.reconnect_count,
            "resync_count": self.resync_count,
            "orderbook_sync_state": self.orderbook_sync_state.value,
            "sequence_integrity_state": self.sequence_integrity_state.value,
            "queue_lag": self.queue_lag,
            "tick_queue_lag": self.tick_queue_lag,
            "snapshot_queue_lag": self.snapshot_queue_lag,
            "dropped_trades": self.dropped_trades,
            "dropped_requeued_trades": self.dropped_requeued_trades,
            "dropped_snapshots": self.dropped_snapshots,
            "trades_received": self.trades_received,
            "depth_updates_received": self.depth_updates_received,
            "snapshots_published": self.snapshots_published,
            "last_update_id": self.last_update_id,
            "last_error": self.last_error,
        }


@dataclass(slots=True)
class MarketDataHealth:
    status: MarketDataEngineStatus = MarketDataEngineStatus.STOPPED
    enabled: bool = False
    connected: bool = False
    websocket_url: str | None = None
    reconnect_count: int = 0
    symbols: dict[str, SymbolMarketDataHealth] = field(default_factory=dict)
    last_error: str | None = None
    degraded_reasons: list[str] = field(default_factory=list)
    task_statuses: dict[str, str] = field(default_factory=dict)
    redis_publish_failures: int = 0
    alert_publish_failures: int = 0
    market_event_publish_failures: int = 0
    clickhouse_flush_failures: int = 0
    last_clickhouse_write_ts: int | None = None
    last_clickhouse_error: str | None = None
    last_redis_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "enabled": self.enabled,
            "connected": self.connected,
            "websocket_url": self.websocket_url,
            "reconnect_count": self.reconnect_count,
            "symbols": {symbol: health.to_dict() for symbol, health in self.symbols.items()},
            "last_error": self.last_error,
            "degraded_reasons": self.degraded_reasons,
            "task_statuses": self.task_statuses,
            "redis_publish_failures": self.redis_publish_failures,
            "alert_publish_failures": self.alert_publish_failures,
            "market_event_publish_failures": self.market_event_publish_failures,
            "clickhouse_flush_failures": self.clickhouse_flush_failures,
            "last_clickhouse_write_ts": self.last_clickhouse_write_ts,
            "last_clickhouse_error": self.last_clickhouse_error,
            "last_redis_error": self.last_redis_error,
        }
