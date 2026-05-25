from __future__ import annotations

import asyncio
import secrets
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Protocol

from core.config import AppSettings
from core.event_bus import EventBus
from core.events import EventEnvelope, EventType
from core.logging import LogCategory, get_logger
from core.streams import MARKET_ORDERBOOK_STREAM, MARKET_TICKS_STREAM, SYSTEM_ALERTS_STREAM
from market_data.backpressure import MarketDataQueues
from market_data.binance_client import BinanceRateLimitError, BinanceSpotClient
from market_data.clock import now_ms
from market_data.models import (
    ApplyUpdateResult,
    DepthUpdate,
    MarketDataEngineStatus,
    MarketDataHealth,
    NormalizedTrade,
    OrderBookSyncState,
    ReducedOrderBookSnapshot,
    SequenceIntegrityState,
    SymbolMarketDataHealth,
)
from market_data.normalizers import normalize_depth_update, normalize_trade
from market_data.orderbook import LocalOrderBook
from storage.market_data_writer import ClickHouseMarketDataWriter


class MarketDataWriter(Protocol):
    async def ensure_schema(self) -> None:
        ...

    async def insert_trades(self, trades: list[NormalizedTrade]) -> None:
        ...

    async def insert_orderbook_snapshots(self, snapshots: list[ReducedOrderBookSnapshot]) -> None:
        ...

    async def close(self) -> None:
        ...


class MarketDataEngine:
    def __init__(
        self,
        settings: AppSettings,
        event_bus: EventBus,
        *,
        client: BinanceSpotClient | None = None,
        writer: MarketDataWriter | None = None,
    ) -> None:
        self._settings = settings.market_data
        self._event_bus = event_bus
        self._client = client or BinanceSpotClient(self._settings)
        self._writer = writer or ClickHouseMarketDataWriter(settings.clickhouse, settings.retry)
        self._queues = MarketDataQueues(self._settings)
        self._orderbooks = {
            symbol: LocalOrderBook(
                symbol,
                levels=self._settings.orderbook_levels,
                max_buffered_updates=self._settings.queue_max_size_per_symbol,
            )
            for symbol in self._settings.symbols
        }
        self._symbol_locks = {symbol: asyncio.Lock() for symbol in self._settings.symbols}
        self._resync_tasks: dict[str, asyncio.Task[None]] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self._stop_event = asyncio.Event()
        self._stopping = False
        self._last_snapshot_publish_ms = {symbol: 0 for symbol in self._settings.symbols}
        self._last_resync_attempt_ms = {symbol: 0 for symbol in self._settings.symbols}
        self._degraded_reasons: set[str] = set()
        self._health = MarketDataHealth(
            enabled=self._settings.enabled,
            websocket_url=self._client.build_multiplex_url(),
            symbols={
                symbol: SymbolMarketDataHealth(symbol=symbol)
                for symbol in self._settings.symbols
            },
        )
        self._logger = get_logger(__name__)

    async def start(self) -> None:
        if not self._settings.enabled:
            self._health.status = MarketDataEngineStatus.STOPPED
            return

        self._health.status = MarketDataEngineStatus.STARTING
        self._stopping = False
        self._stop_event.clear()
        self._queues.start_accepting()
        try:
            await asyncio.wait_for(
                self._writer.ensure_schema(),
                timeout=self._settings.clickhouse_flush_timeout_seconds,
            )
            self._clear_degraded_reason("clickhouse_schema_unavailable")
        except Exception as exc:
            self._set_degraded("clickhouse_schema_unavailable", str(exc))
            self._logger.error(
                "ClickHouse schema initialization failed; market data will run degraded",
                extra={"category": LogCategory.ERROR.value},
                exc_info=True,
            )
        self._tasks = [
            asyncio.create_task(self._websocket_loop(), name="market-data-websocket"),
            asyncio.create_task(self._trade_writer_loop(), name="market-data-trade-writer"),
            asyncio.create_task(self._snapshot_writer_loop(), name="market-data-orderbook-writer"),
        ]

    async def stop(self) -> None:
        if self._health.status == MarketDataEngineStatus.STOPPED:
            return

        self._health.status = MarketDataEngineStatus.STOPPING
        self._stopping = True
        self._stop_event.set()
        self._queues.stop_accepting()

        await self._cancel_resync_tasks()

        websocket_tasks = [task for task in self._tasks if "websocket" in task.get_name()]
        for task in websocket_tasks:
            task.cancel()
        if websocket_tasks:
            await asyncio.gather(*websocket_tasks, return_exceptions=True)

        writer_tasks = [task for task in self._tasks if "writer" in task.get_name()]
        if writer_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*writer_tasks, return_exceptions=True),
                    timeout=self._settings.shutdown_drain_timeout_seconds,
                )
            except TimeoutError:
                for task in writer_tasks:
                    task.cancel()
                await asyncio.gather(*writer_tasks, return_exceptions=True)

        await self._flush_final()
        with suppress(Exception):
            await self._client.close()
        with suppress(Exception):
            await self._writer.close()
        self._tasks.clear()
        self._resync_tasks.clear()
        self._health.connected = False
        self._health.status = MarketDataEngineStatus.STOPPED

    async def close(self) -> None:
        await self.stop()

    async def ping(self) -> bool:
        if not self._settings.enabled:
            return True
        self._refresh_health_fields()
        tasks_healthy = all(
            status != "failed" for status in self._health.task_statuses.values()
        ) and all(
            status == "running"
            for name, status in self._health.task_statuses.items()
            if name
            in {
                "market-data-websocket",
                "market-data-trade-writer",
                "market-data-orderbook-writer",
            }
        )
        symbols_healthy = all(
            symbol_health.orderbook_sync_state == OrderBookSyncState.SYNCED
            and symbol_health.last_event_age_ms is not None
            and symbol_health.last_event_age_ms <= self._settings.health_stale_after_ms
            for symbol_health in self._health.symbols.values()
        )
        return (
            self._health.status == MarketDataEngineStatus.RUNNING
            and self._health.connected
            and tasks_healthy
            and symbols_healthy
            and not self._degraded_reasons
        )

    def health_snapshot(self) -> dict[str, object]:
        self._refresh_health_fields()
        return self._health.to_dict()

    def _refresh_health_fields(self) -> None:
        current_ms = now_ms()
        for symbol in self._settings.symbols:
            queue_lag = self._queues.lag_for_symbol(symbol)
            symbol_health = self._health.symbols[symbol]
            symbol_health.queue_lag = queue_lag.total
            symbol_health.tick_queue_lag = queue_lag.tick_queue_lag
            symbol_health.snapshot_queue_lag = queue_lag.snapshot_queue_lag
            symbol_health.dropped_trades = queue_lag.dropped_trades
            symbol_health.dropped_requeued_trades = queue_lag.dropped_requeued_trades
            symbol_health.dropped_snapshots = queue_lag.dropped_snapshots
            symbol_health.last_event_age_ms = (
                current_ms - symbol_health.last_event_received_ts
                if symbol_health.last_event_received_ts is not None
                else None
            )
            symbol_health.last_update_id = self._orderbooks[symbol].last_update_id
            symbol_health.orderbook_sync_state = self._orderbooks[symbol].sync_state
            symbol_health.sequence_integrity_state = self._orderbooks[symbol].sequence_integrity
            stale_reason = f"stale_market_data:{symbol}"
            if (
                symbol_health.last_event_age_ms is not None
                and symbol_health.last_event_age_ms > self._settings.health_stale_after_ms
            ):
                self._degraded_reasons.add(stale_reason)
            else:
                self._degraded_reasons.discard(stale_reason)
            unsynced_reason = f"orderbook_unsynced:{symbol}"
            if (
                self._health.connected
                and symbol_health.orderbook_sync_state != OrderBookSyncState.SYNCED
            ):
                self._degraded_reasons.add(unsynced_reason)
            elif symbol_health.orderbook_sync_state == OrderBookSyncState.SYNCED:
                self._degraded_reasons.discard(unsynced_reason)
        self._health.degraded_reasons = sorted(self._degraded_reasons)
        self._health.task_statuses = self._task_statuses()
        self._refresh_engine_status()

    async def _websocket_loop(self) -> None:
        reconnect_delay = self._settings.reconnect_base_delay_seconds
        while not self._stop_event.is_set():
            try:
                await self._run_websocket_once()
                reconnect_delay = self._settings.reconnect_base_delay_seconds
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self._cancel_resync_tasks()
                self._set_degraded("websocket_disconnected", str(exc))
                self._health.connected = False
                self._health.last_error = str(exc)
                self._health.reconnect_count += 1
                for symbol_health in self._health.symbols.values():
                    symbol_health.connected = False
                    symbol_health.reconnect_count += 1
                    symbol_health.last_error = str(exc)
                await self._safe_publish_alert(
                    "binance_websocket_reconnect",
                    None,
                    {"error": str(exc), "reconnect_count": self._health.reconnect_count},
                )
                self._logger.warning(
                    "Binance websocket reconnect scheduled",
                    extra={
                        "category": LogCategory.SYSTEM.value,
                        "latency_ms": None,
                        "reconnect_delay_seconds": reconnect_delay,
                    },
                    exc_info=True,
                )
                await asyncio.sleep(self._with_jitter(reconnect_delay))
                reconnect_delay = min(
                    reconnect_delay * 2,
                    self._settings.reconnect_max_delay_seconds,
                )

    async def _run_websocket_once(self) -> None:
        await self._cancel_resync_tasks()
        for symbol in self._settings.symbols:
            async with self._symbol_locks[symbol]:
                self._orderbooks[symbol].start_buffering()

        async with self._client.connect() as websocket:
            self._health.connected = True
            self._clear_degraded_reason("websocket_disconnected")
            self._refresh_engine_status()
            for symbol_health in self._health.symbols.values():
                symbol_health.connected = True
                symbol_health.last_error = None

            for symbol in self._settings.symbols:
                self._resync_tasks[symbol] = asyncio.create_task(
                    self._sync_orderbook(symbol),
                    name=f"market-data-sync-{symbol}",
                )

            while not self._stop_event.is_set():
                payload = await self._client.recv_combined_payload(websocket)
                event_type = payload.get("e")
                if event_type == "serverShutdown":
                    raise ConnectionError("Binance serverShutdown event received")
                if event_type == "trade":
                    await self._handle_trade(payload)
                elif event_type == "depthUpdate":
                    await self._handle_depth_update(payload)

    async def _handle_trade(self, payload: dict[str, object]) -> None:
        trade = normalize_trade(payload)
        self._mark_symbol_event(trade.symbol, trade.received_ts, event_kind="trade")
        await self._publish_trade(trade)
        put_result = await self._queues.put_trade(trade)
        if put_result.had_backpressure:
            await self._safe_publish_alert(
                "tick_queue_backpressure",
                trade.symbol,
                {
                    "queue_lag": self._queues.lag_for_symbol(trade.symbol).tick_queue_lag,
                    "accepted": put_result.accepted,
                    "dropped_trade": put_result.dropped_trade,
                    "reason": put_result.reason,
                },
            )
        if put_result.dropped_trade:
            self._logger.warning(
                "Market data trade queue overflow handled",
                extra={
                    "category": LogCategory.SYSTEM.value,
                    "symbol": trade.symbol,
                    "reason": put_result.reason,
                    "accepted": put_result.accepted,
                },
            )

    async def _handle_depth_update(self, payload: dict[str, object]) -> None:
        update = normalize_depth_update(payload)
        self._mark_symbol_event(update.symbol, update.received_ts, event_kind="depth")

        failure: tuple[DepthUpdate, ApplyUpdateResult] | None = None
        snapshot: ReducedOrderBookSnapshot | None = None
        async with self._symbol_locks[update.symbol]:
            orderbook = self._orderbooks[update.symbol]
            result = orderbook.apply_update(update)
            if result == ApplyUpdateResult.BUFFERED:
                return
            if result in {ApplyUpdateResult.GAP_DETECTED, ApplyUpdateResult.INVALID_SEQUENCE}:
                failure = (update, result)
            if result == ApplyUpdateResult.IGNORED_OLD:
                return

            if failure is None and not self._is_snapshot_due(update.symbol, update.processed_ts):
                return

            if failure is None:
                snapshot = orderbook.reduced_snapshot(update)
            if failure is None and snapshot is None:
                return

            if snapshot is not None:
                self._last_snapshot_publish_ms[update.symbol] = update.processed_ts

        if failure is not None:
            await self._handle_sequence_failure(*failure)
            return

        if snapshot is None:
            return

        await self._publish_orderbook(snapshot)
        dropped_old_snapshot = self._queues.put_snapshot(snapshot)
        self._health.symbols[snapshot.symbol].last_snapshot_ts = snapshot.processed_ts
        self._health.symbols[snapshot.symbol].snapshots_published += 1
        if dropped_old_snapshot:
            await self._safe_publish_alert(
                "orderbook_snapshot_backpressure",
                snapshot.symbol,
                {"queue_lag": self._queues.lag_for_symbol(snapshot.symbol).snapshot_queue_lag},
            )

    async def _sync_orderbook(self, symbol: str) -> None:
        while not self._stop_event.is_set():
            async with self._symbol_locks[symbol]:
                orderbook = self._orderbooks[symbol]
                orderbook.sync_state = OrderBookSyncState.SYNCING
                first_update = orderbook.buffered_updates[0] if orderbook.buffered_updates else None

            if first_update is None:
                await asyncio.sleep(0.05)
                continue

            try:
                await self._respect_resync_cooldown(symbol)
                snapshot = await self._client.fetch_depth_snapshot(symbol)
                self._clear_degraded_reason("binance_rate_limited")
                self._clear_degraded_reason("binance_ip_banned")
            except asyncio.CancelledError:
                raise
            except BinanceRateLimitError as exc:
                reason = "binance_ip_banned" if exc.status_code == 418 else "binance_rate_limited"
                self._set_degraded(reason, str(exc))
                self._health.symbols[symbol].last_error = str(exc)
                await self._safe_publish_alert(
                    "orderbook_snapshot_rate_limited",
                    symbol,
                    {
                        "error": str(exc),
                        "status_code": exc.status_code,
                        "retry_after_seconds": exc.retry_after_seconds,
                    },
                )
                await asyncio.sleep(self._with_jitter(exc.retry_after_seconds))
                continue
            except Exception as exc:
                self._set_degraded("orderbook_snapshot_fetch_failed", str(exc))
                self._health.symbols[symbol].last_error = str(exc)
                await self._safe_publish_alert(
                    "orderbook_snapshot_fetch_failed",
                    symbol,
                    {"error": str(exc)},
                )
                await asyncio.sleep(self._with_jitter(self._settings.reconnect_base_delay_seconds))
                continue

            async with self._symbol_locks[symbol]:
                orderbook = self._orderbooks[symbol]
                current_first = (
                    orderbook.buffered_updates[0]
                    if orderbook.buffered_updates
                    else first_update
                )
                if snapshot.last_update_id < current_first.first_update_id:
                    continue
                result = orderbook.initialize_from_snapshot(snapshot)
                self._health.symbols[symbol].orderbook_sync_state = orderbook.sync_state
                self._health.symbols[symbol].sequence_integrity_state = orderbook.sequence_integrity
                self._health.symbols[symbol].last_update_id = orderbook.last_update_id

            if result == ApplyUpdateResult.APPLIED:
                self._clear_degraded_reason("orderbook_snapshot_fetch_failed")
                self._clear_degraded_reason(f"orderbook_unsynced:{symbol}")
                self._logger.info(
                    "Binance orderbook synchronized",
                    extra={
                        "category": LogCategory.SYSTEM.value,
                        "symbol": symbol,
                        "last_update_id": orderbook.last_update_id,
                    },
                )
                return

            self._set_degraded(f"orderbook_unsynced:{symbol}")
            await self._safe_publish_alert(
                "orderbook_initial_sync_failed",
                symbol,
                {"result": result.value, "snapshot_last_update_id": snapshot.last_update_id},
            )
            async with self._symbol_locks[symbol]:
                self._orderbooks[symbol].start_buffering()

    async def _handle_sequence_failure(
        self,
        update: DepthUpdate,
        result: ApplyUpdateResult,
    ) -> None:
        self._set_degraded(f"orderbook_unsynced:{update.symbol}", result.value)
        await self._safe_publish_alert(
            "orderbook_sequence_gap",
            update.symbol,
            {
                "sequence_failure": result.value,
                "event_first_update_id": update.first_update_id,
                "event_final_update_id": update.final_update_id,
                "action": "resync_orderbook",
            },
        )
        await self._trigger_resync(update.symbol)

    async def _trigger_resync(self, symbol: str) -> None:
        task = self._resync_tasks.get(symbol)
        if task is not None and not task.done():
            return
        async with self._symbol_locks[symbol]:
            self._orderbooks[symbol].start_buffering(
                sync_state=OrderBookSyncState.RESYNCING,
                sequence_integrity=self._orderbooks[symbol].sequence_integrity,
            )
        self._health.symbols[symbol].resync_count += 1
        self._resync_tasks[symbol] = asyncio.create_task(
            self._sync_orderbook(symbol),
            name=f"market-data-resync-{symbol}",
        )

    async def _cancel_resync_tasks(self) -> None:
        tasks = [task for task in self._resync_tasks.values() if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._resync_tasks.clear()

    async def _trade_writer_loop(self) -> None:
        await self._writer_loop(self._flush_trades_once)

    async def _snapshot_writer_loop(self) -> None:
        await self._writer_loop(self._flush_snapshots_once)

    async def _writer_loop(self, flush_once: Callable[[], Awaitable[None]]) -> None:
        interval_seconds = self._settings.clickhouse_flush_interval_ms / 1000
        while not self._stop_event.is_set():
            try:
                await flush_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._set_degraded("clickhouse_flush_failed", str(exc))
                self._health.clickhouse_flush_failures += 1
                self._health.last_clickhouse_error = str(exc)
                self._logger.error(
                    "ClickHouse market data flush failed",
                    extra={"category": LogCategory.ERROR.value},
                    exc_info=True,
                )
                await self._safe_publish_alert(
                    "clickhouse_market_data_flush_failed",
                    None,
                    {"error": str(exc)},
                )
            await asyncio.sleep(interval_seconds)

    async def _flush_trades_once(self) -> None:
        trades: list[NormalizedTrade] = []
        per_symbol_limit = max(1, self._settings.clickhouse_batch_size // len(self._settings.symbols))
        for symbol in self._settings.symbols:
            trades.extend(self._queues.drain_trades(symbol, per_symbol_limit))
        if not trades:
            return
        try:
            await self._writer.insert_trades(trades)
            self._health.last_clickhouse_write_ts = now_ms()
            self._health.last_clickhouse_error = None
            self._clear_degraded_reason("clickhouse_schema_unavailable")
            self._clear_degraded_reason("clickhouse_flush_failed")
        except Exception:
            dropped = self._queues.requeue_trades(trades)
            if dropped:
                self._logger.warning(
                    "Dropped trades after ClickHouse flush failure",
                    extra={
                        "category": LogCategory.SYSTEM.value,
                        "dropped_trades": dropped,
                    },
                )
            raise

    async def _flush_snapshots_once(self) -> None:
        snapshots: list[ReducedOrderBookSnapshot] = []
        per_symbol_limit = max(1, self._settings.clickhouse_batch_size // len(self._settings.symbols))
        for symbol in self._settings.symbols:
            snapshots.extend(self._queues.drain_snapshots(symbol, per_symbol_limit))
        if not snapshots:
            return
        try:
            await self._writer.insert_orderbook_snapshots(snapshots)
            self._health.last_clickhouse_write_ts = now_ms()
            self._health.last_clickhouse_error = None
            self._clear_degraded_reason("clickhouse_schema_unavailable")
            self._clear_degraded_reason("clickhouse_flush_failed")
        except Exception:
            self._queues.requeue_latest_snapshots(snapshots)
            raise

    async def _publish_trade(self, trade: NormalizedTrade) -> None:
        event = EventEnvelope(
            event_type=EventType.MARKET_TICK,
            timestamp=trade.processed_ts,
            source="market_data_engine",
            symbol=trade.symbol,
            payload=trade.to_payload(),
        )
        await self._safe_publish_market_event(MARKET_TICKS_STREAM, event, trade.symbol)

    async def _publish_orderbook(self, snapshot: ReducedOrderBookSnapshot) -> None:
        event = EventEnvelope(
            event_type=EventType.ORDERBOOK_UPDATE,
            timestamp=snapshot.processed_ts,
            source="market_data_engine",
            symbol=snapshot.symbol,
            payload=snapshot.to_payload(),
        )
        await self._safe_publish_market_event(MARKET_ORDERBOOK_STREAM, event, snapshot.symbol)

    async def _safe_publish_market_event(
        self,
        stream: str,
        event: EventEnvelope,
        symbol: str | None,
    ) -> bool:
        try:
            await self._event_bus.publish(stream, event)
            self._health.last_redis_error = None
            self._clear_degraded_reason("redis_publish_failed")
            self._clear_degraded_reason("alert_publish_failed")
            return True
        except Exception as exc:
            self._set_degraded("redis_publish_failed", str(exc))
            self._health.redis_publish_failures += 1
            self._health.market_event_publish_failures += 1
            self._health.last_redis_error = str(exc)
            self._logger.error(
                "Redis market event publish failed; continuing in degraded mode",
                extra={
                    "category": LogCategory.ERROR.value,
                    "stream": stream,
                    "symbol": symbol,
                    "event_type": event.event_type.value,
                },
                exc_info=True,
            )
            return False

    async def _safe_publish_alert(
        self,
        reason: str,
        symbol: str | None,
        payload: dict[str, object],
    ) -> bool:
        alert_payload = {
            **payload,
            "reason": reason,
            "source_exchange": self._settings.source_exchange,
        }
        event = EventEnvelope(
            event_type=EventType.SYSTEM_ALERT,
            timestamp=now_ms(),
            source="market_data_engine",
            symbol=symbol,
            payload=alert_payload,
        )
        try:
            await self._event_bus.publish(SYSTEM_ALERTS_STREAM, event)
            self._health.last_redis_error = None
            self._clear_degraded_reason("redis_publish_failed")
            self._clear_degraded_reason("alert_publish_failed")
            return True
        except Exception as exc:
            self._set_degraded("alert_publish_failed", str(exc))
            self._health.redis_publish_failures += 1
            self._health.alert_publish_failures += 1
            self._health.last_redis_error = str(exc)
            self._logger.error(
                "Redis alert publish failed; continuing with local structured logs",
                extra={
                    "category": LogCategory.ERROR.value,
                    "alert_reason": reason,
                    "symbol": symbol,
                    "alert_payload": alert_payload,
                },
                exc_info=True,
            )
            return False

    def _mark_symbol_event(self, symbol: str, received_ts: int, *, event_kind: str) -> None:
        symbol_health = self._health.symbols[symbol]
        symbol_health.connected = True
        symbol_health.last_event_received_ts = received_ts
        if event_kind == "trade":
            symbol_health.last_trade_ts = received_ts
            symbol_health.trades_received += 1
        elif event_kind == "depth":
            symbol_health.last_depth_ts = received_ts
            symbol_health.depth_updates_received += 1

    def _is_snapshot_due(self, symbol: str, processed_ts: int) -> bool:
        return (
            processed_ts - self._last_snapshot_publish_ms[symbol]
            >= self._settings.orderbook_snapshot_interval_ms
        )

    async def _respect_resync_cooldown(self, symbol: str) -> None:
        current_ms = now_ms()
        elapsed_ms = current_ms - self._last_resync_attempt_ms[symbol]
        remaining_ms = self._settings.resync_cooldown_ms - elapsed_ms
        if remaining_ms > 0:
            await asyncio.sleep(remaining_ms / 1000)
        self._last_resync_attempt_ms[symbol] = now_ms()

    async def _flush_final(self) -> None:
        try:
            await asyncio.wait_for(
                self._flush_trades_once(),
                timeout=self._settings.shutdown_drain_timeout_seconds,
            )
            await asyncio.wait_for(
                self._flush_snapshots_once(),
                timeout=self._settings.shutdown_drain_timeout_seconds,
            )
        except Exception as exc:
            self._set_degraded("shutdown_flush_failed", str(exc))
            self._logger.warning(
                "Final market data flush did not complete cleanly",
                extra={"category": LogCategory.SYSTEM.value},
                exc_info=True,
            )

    def _set_degraded(self, reason: str, error: str | None = None) -> None:
        self._degraded_reasons.add(reason)
        self._health.degraded_reasons = sorted(self._degraded_reasons)
        if error is not None:
            self._health.last_error = error
        self._refresh_engine_status()

    def _clear_degraded_reason(self, reason: str) -> None:
        self._degraded_reasons.discard(reason)
        self._health.degraded_reasons = sorted(self._degraded_reasons)
        self._refresh_engine_status()

    def _refresh_engine_status(self) -> None:
        if self._health.status in {MarketDataEngineStatus.STOPPED, MarketDataEngineStatus.STOPPING}:
            return
        if self._degraded_reasons:
            self._health.status = MarketDataEngineStatus.DEGRADED
        elif self._health.connected:
            self._health.status = MarketDataEngineStatus.RUNNING
            self._health.last_error = None

    def _task_statuses(self) -> dict[str, str]:
        statuses: dict[str, str] = {}
        for task in [*self._tasks, *self._resync_tasks.values()]:
            name = task.get_name()
            if task.cancelled():
                statuses[name] = "cancelled"
            elif task.done():
                statuses[name] = "failed" if task.exception() is not None else "done"
            else:
                statuses[name] = "running"
        return statuses

    def _with_jitter(self, delay_seconds: float) -> float:
        if self._settings.reconnect_jitter_seconds == 0:
            return delay_seconds
        jitter_ms = int(self._settings.reconnect_jitter_seconds * 1000)
        if jitter_ms <= 0:
            return delay_seconds
        return delay_seconds + secrets.randbelow(jitter_ms + 1) / 1000
