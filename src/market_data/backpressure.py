from __future__ import annotations

import asyncio
from dataclasses import dataclass

from core.config import MarketDataSettings
from market_data.models import NormalizedTrade, ReducedOrderBookSnapshot


@dataclass(frozen=True, slots=True)
class QueueLag:
    tick_queue_lag: int
    snapshot_queue_lag: int
    dropped_trades: int
    dropped_requeued_trades: int
    dropped_snapshots: int

    @property
    def total(self) -> int:
        return self.tick_queue_lag + self.snapshot_queue_lag


@dataclass(frozen=True, slots=True)
class TradeQueuePutResult:
    accepted: bool
    had_backpressure: bool = False
    dropped_trade: bool = False
    reason: str | None = None


class LatestSnapshotQueue:
    def __init__(self, max_size: int) -> None:
        self._queue: asyncio.Queue[ReducedOrderBookSnapshot] = asyncio.Queue(maxsize=max_size)

    def put_latest(self, snapshot: ReducedOrderBookSnapshot) -> bool:
        dropped = False
        if self._queue.full():
            self._queue.get_nowait()
            self._queue.task_done()
            dropped = True
        self._queue.put_nowait(snapshot)
        return dropped

    def drain(self, max_items: int) -> list[ReducedOrderBookSnapshot]:
        drained: list[ReducedOrderBookSnapshot] = []
        while len(drained) < max_items and not self._queue.empty():
            drained.append(self._queue.get_nowait())
            self._queue.task_done()
        return drained

    def qsize(self) -> int:
        return self._queue.qsize()


class MarketDataQueues:
    def __init__(self, settings: MarketDataSettings) -> None:
        self._settings = settings
        self._accepting = True
        self._trade_queues = {
            symbol: asyncio.Queue[NormalizedTrade](maxsize=settings.queue_max_size_per_symbol)
            for symbol in settings.symbols
        }
        self._snapshot_queues = {
            symbol: LatestSnapshotQueue(settings.snapshot_queue_max_size_per_symbol)
            for symbol in settings.symbols
        }
        self._dropped_trades = {symbol: 0 for symbol in settings.symbols}
        self._dropped_requeued_trades = {symbol: 0 for symbol in settings.symbols}
        self._dropped_snapshots = {symbol: 0 for symbol in settings.symbols}

    async def put_trade(self, trade: NormalizedTrade) -> TradeQueuePutResult:
        if not self._accepting:
            self._dropped_trades[trade.symbol] += 1
            return TradeQueuePutResult(
                accepted=False,
                dropped_trade=True,
                reason="queue_closed",
            )

        queue = self._trade_queues[trade.symbol]
        if not queue.full():
            queue.put_nowait(trade)
            return TradeQueuePutResult(accepted=True)

        try:
            await asyncio.wait_for(
                queue.put(trade),
                timeout=self._settings.trade_queue_put_timeout_ms / 1000,
            )
            return TradeQueuePutResult(accepted=True, had_backpressure=True)
        except TimeoutError:
            self._drop_oldest_trade(trade.symbol)

        try:
            queue.put_nowait(trade)
            return TradeQueuePutResult(
                accepted=True,
                had_backpressure=True,
                dropped_trade=True,
                reason="drop_oldest",
            )
        except asyncio.QueueFull:
            self._dropped_trades[trade.symbol] += 1
            return TradeQueuePutResult(
                accepted=False,
                had_backpressure=True,
                dropped_trade=True,
                reason="queue_full",
            )

    def put_snapshot(self, snapshot: ReducedOrderBookSnapshot) -> bool:
        dropped = self._snapshot_queues[snapshot.symbol].put_latest(snapshot)
        if dropped:
            self._dropped_snapshots[snapshot.symbol] += 1
        return dropped

    def drain_trades(self, symbol: str, max_items: int) -> list[NormalizedTrade]:
        queue = self._trade_queues[symbol]
        drained: list[NormalizedTrade] = []
        while len(drained) < max_items and not queue.empty():
            drained.append(queue.get_nowait())
            queue.task_done()
        return drained

    def drain_snapshots(self, symbol: str, max_items: int) -> list[ReducedOrderBookSnapshot]:
        return self._snapshot_queues[symbol].drain(max_items)

    def requeue_trades(self, trades: list[NormalizedTrade]) -> int:
        dropped = 0
        for trade in trades:
            queue = self._trade_queues[trade.symbol]
            if not self._accepting or queue.full():
                self._dropped_requeued_trades[trade.symbol] += 1
                dropped += 1
                continue
            queue.put_nowait(trade)
        return dropped

    def requeue_latest_snapshots(self, snapshots: list[ReducedOrderBookSnapshot]) -> None:
        for snapshot in snapshots:
            self._snapshot_queues[snapshot.symbol].put_latest(snapshot)

    def stop_accepting(self) -> None:
        self._accepting = False

    def start_accepting(self) -> None:
        self._accepting = True

    def lag_for_symbol(self, symbol: str) -> QueueLag:
        return QueueLag(
            tick_queue_lag=self._trade_queues[symbol].qsize(),
            snapshot_queue_lag=self._snapshot_queues[symbol].qsize(),
            dropped_trades=self._dropped_trades[symbol],
            dropped_requeued_trades=self._dropped_requeued_trades[symbol],
            dropped_snapshots=self._dropped_snapshots[symbol],
        )

    def _drop_oldest_trade(self, symbol: str) -> None:
        queue = self._trade_queues[symbol]
        try:
            queue.get_nowait()
            queue.task_done()
            self._dropped_trades[symbol] += 1
        except asyncio.QueueEmpty:
            return
