import asyncio

import pytest

from core.config import AppSettings, MarketDataSettings
from core.streams import SYSTEM_ALERTS_STREAM
from market_data.engine import MarketDataEngine
from market_data.models import (
    MarketDataEngineStatus,
    OrderBookSyncState,
    RestDepthSnapshot,
    SequenceIntegrityState,
)


class FakeEventBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, object]] = []

    async def ensure_streams(self, streams: tuple[str, ...]) -> None:
        self.streams = streams

    async def ping(self) -> bool:
        return True

    async def publish(self, stream: str, event: object) -> str:
        self.published.append((stream, event))
        return f"{len(self.published)}-0"

    async def read(self, stream: str, group: str, consumer: str, count: int = 10) -> list[object]:
        return []

    async def ack(self, stream: str, group: str, message_id: str) -> int:
        return 1

    async def close(self) -> None:
        return None


class FailingEventBus(FakeEventBus):
    async def publish(self, stream: str, event: object) -> str:
        raise ConnectionError("redis unavailable")


class FakeBinanceClient:
    def __init__(self) -> None:
        self.closed = False

    def build_multiplex_url(self) -> str:
        return "wss://example.test/stream?streams=btcusdt@trade/btcusdt@depth@100ms"

    async def fetch_depth_snapshot(self, symbol: str) -> RestDepthSnapshot:
        await asyncio.sleep(3600)
        raise AssertionError(f"unexpected fetch completion for {symbol}")

    async def close(self) -> None:
        self.closed = True


class FakeWriter:
    def __init__(self) -> None:
        self.closed = False

    async def ensure_schema(self) -> None:
        return None

    async def insert_trades(self, trades: list[object]) -> None:
        return None

    async def insert_orderbook_snapshots(self, snapshots: list[object]) -> None:
        return None

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_sequence_gap_publishes_alert_and_schedules_resync() -> None:
    settings = AppSettings(
        market_data=MarketDataSettings(
            enabled=True,
            symbols=("BTCUSDT",),
            orderbook_snapshot_interval_ms=100,
        )
    )
    event_bus = FakeEventBus()
    engine = MarketDataEngine(
        settings,
        event_bus,
        client=FakeBinanceClient(),
        writer=FakeWriter(),
    )

    orderbook = engine._orderbooks["BTCUSDT"]
    orderbook.initialize_from_snapshot(
        RestDepthSnapshot(
            received_ts=1,
            processed_ts=1,
            symbol="BTCUSDT",
            source_exchange="binance_spot",
            last_update_id=10,
            bids=(("100.0", "1.0"),),
            asks=(("101.0", "1.0"),),
        )
    )

    await engine._handle_depth_update(
        {
            "e": "depthUpdate",
            "E": 1_000,
            "s": "BTCUSDT",
            "U": 12,
            "u": 12,
            "b": [["100.0", "1.1"]],
            "a": [["101.0", "1.2"]],
        }
    )

    alert_events = [event for stream, event in event_bus.published if stream == SYSTEM_ALERTS_STREAM]
    assert alert_events
    assert alert_events[-1].payload["reason"] == "orderbook_sequence_gap"
    assert "BTCUSDT" in engine._resync_tasks
    assert orderbook.sequence_integrity == SequenceIntegrityState.GAP_DETECTED
    assert orderbook.sync_state in {OrderBookSyncState.UNSYNCED, OrderBookSyncState.RESYNCING}

    await engine.stop()


@pytest.mark.anyio
async def test_trade_handler_blocks_instead_of_silently_dropping_ticks() -> None:
    settings = AppSettings(
        market_data=MarketDataSettings(
            enabled=True,
            symbols=("BTCUSDT",),
            queue_max_size_per_symbol=100,
        )
    )
    event_bus = FakeEventBus()
    engine = MarketDataEngine(
        settings,
        event_bus,
        client=FakeBinanceClient(),
        writer=FakeWriter(),
    )

    await engine._handle_trade(
        {
            "e": "trade",
            "E": 1_000,
            "s": "BTCUSDT",
            "t": 123,
            "p": "100.0",
            "q": "0.5",
            "T": 1_000,
            "m": False,
        }
    )

    assert engine._queues.lag_for_symbol("BTCUSDT").tick_queue_lag == 1
    assert not [event for stream, event in event_bus.published if stream == SYSTEM_ALERTS_STREAM]

    await engine.stop()


@pytest.mark.anyio
async def test_redis_publish_failure_does_not_kill_trade_handler() -> None:
    settings = AppSettings(
        market_data=MarketDataSettings(
            enabled=True,
            symbols=("BTCUSDT",),
            queue_max_size_per_symbol=100,
        )
    )
    engine = MarketDataEngine(
        settings,
        FailingEventBus(),
        client=FakeBinanceClient(),
        writer=FakeWriter(),
    )
    engine._health.status = MarketDataEngineStatus.RUNNING
    engine._health.connected = True

    await engine._handle_trade(
        {
            "e": "trade",
            "E": 1_000,
            "s": "BTCUSDT",
            "t": 123,
            "p": "100.0",
            "q": "0.5",
            "T": 1_000,
            "m": False,
        }
    )

    health = engine.health_snapshot()

    assert health["status"] == "degraded"
    assert "redis_publish_failed" in health["degraded_reasons"]
    assert health["redis_publish_failures"] == 1
    assert health["market_event_publish_failures"] == 1
    assert health["symbols"]["BTCUSDT"]["tick_queue_lag"] == 1

    await engine.stop()
