from __future__ import annotations

from collections import deque

from market_data.clock import latency_ms, now_ms
from market_data.models import (
    ApplyUpdateResult,
    DepthUpdate,
    OrderBookSyncState,
    ReducedOrderBookSnapshot,
    RestDepthSnapshot,
    SequenceIntegrityState,
)


class LocalOrderBook:
    def __init__(self, symbol: str, levels: int, max_buffered_updates: int) -> None:
        self.symbol = symbol.upper()
        self.levels = levels
        self.max_buffered_updates = max_buffered_updates
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.last_update_id: int | None = None
        self.sync_state = OrderBookSyncState.UNSYNCED
        self.sequence_integrity = SequenceIntegrityState.UNKNOWN
        self.buffered_updates: deque[DepthUpdate] = deque()

    def start_buffering(
        self,
        *,
        sync_state: OrderBookSyncState = OrderBookSyncState.BUFFERING,
        sequence_integrity: SequenceIntegrityState = SequenceIntegrityState.UNKNOWN,
    ) -> None:
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = None
        self.sync_state = sync_state
        self.sequence_integrity = sequence_integrity
        self.buffered_updates.clear()

    def buffer_update(self, update: DepthUpdate) -> bool:
        if len(self.buffered_updates) >= self.max_buffered_updates:
            self.invalidate(SequenceIntegrityState.GAP_DETECTED)
            return False
        self.buffered_updates.append(update)
        return True

    def initialize_from_snapshot(self, snapshot: RestDepthSnapshot) -> ApplyUpdateResult:
        self.sync_state = OrderBookSyncState.SYNCING
        first_update = self.buffered_updates[0] if self.buffered_updates else None
        if first_update is not None and snapshot.last_update_id < first_update.first_update_id:
            return ApplyUpdateResult.IGNORED_OLD

        self.bids = _levels_to_book(snapshot.bids)
        self.asks = _levels_to_book(snapshot.asks)
        self.last_update_id = snapshot.last_update_id

        buffered = [event for event in self.buffered_updates if event.final_update_id > snapshot.last_update_id]
        self.buffered_updates.clear()

        if not buffered:
            self.sync_state = OrderBookSyncState.SYNCED
            self.sequence_integrity = SequenceIntegrityState.OK
            return ApplyUpdateResult.APPLIED

        first_applicable = buffered[0]
        if not self._snapshot_bridges_first_event(first_applicable):
            self.invalidate(SequenceIntegrityState.GAP_DETECTED)
            return ApplyUpdateResult.GAP_DETECTED

        self.sync_state = OrderBookSyncState.SYNCED
        for update in buffered:
            result = self.apply_update(update)
            if result in {ApplyUpdateResult.GAP_DETECTED, ApplyUpdateResult.INVALID_SEQUENCE}:
                return result

        self.sync_state = OrderBookSyncState.SYNCED
        self.sequence_integrity = SequenceIntegrityState.OK
        return ApplyUpdateResult.APPLIED

    def apply_update(self, update: DepthUpdate) -> ApplyUpdateResult:
        if update.final_update_id < update.first_update_id:
            self.invalidate(SequenceIntegrityState.INVALID_SEQUENCE)
            return ApplyUpdateResult.INVALID_SEQUENCE

        if self.sync_state != OrderBookSyncState.SYNCED or self.last_update_id is None:
            if not self.buffer_update(update):
                return ApplyUpdateResult.GAP_DETECTED
            return ApplyUpdateResult.BUFFERED

        if update.final_update_id <= self.last_update_id:
            return ApplyUpdateResult.IGNORED_OLD

        if update.first_update_id > self.last_update_id + 1:
            self.invalidate(SequenceIntegrityState.GAP_DETECTED)
            return ApplyUpdateResult.GAP_DETECTED

        self._apply_levels(self.bids, update.bids)
        self._apply_levels(self.asks, update.asks)
        self.last_update_id = update.final_update_id
        self.sync_state = OrderBookSyncState.SYNCED
        self.sequence_integrity = SequenceIntegrityState.OK
        return ApplyUpdateResult.APPLIED

    def reduced_snapshot(self, update: DepthUpdate) -> ReducedOrderBookSnapshot | None:
        bid_levels = self.top_bids()
        ask_levels = self.top_asks()
        if not bid_levels or not ask_levels or self.last_update_id is None:
            return None

        processed_ts = now_ms()
        best_bid = float(bid_levels[0][0])
        best_ask = float(ask_levels[0][0])
        return ReducedOrderBookSnapshot(
            exchange_ts=update.exchange_ts,
            received_ts=update.received_ts,
            processed_ts=processed_ts,
            symbol=self.symbol,
            source_exchange=update.source_exchange,
            sequence=update.final_update_id,
            first_update_id=update.first_update_id,
            final_update_id=update.final_update_id,
            last_update_id=self.last_update_id,
            best_bid=best_bid,
            best_ask=best_ask,
            spread=max(best_ask - best_bid, 0.0),
            bid_levels=bid_levels,
            ask_levels=ask_levels,
            levels=self.levels,
            sync_state=self.sync_state,
            sequence_integrity=self.sequence_integrity,
            ingest_latency_ms=latency_ms(update.exchange_ts, processed_ts),
        )

    def top_bids(self) -> tuple[tuple[str, str], ...]:
        return _book_to_levels(self.bids, self.levels, reverse=True)

    def top_asks(self) -> tuple[tuple[str, str], ...]:
        return _book_to_levels(self.asks, self.levels, reverse=False)

    def invalidate(self, reason: SequenceIntegrityState) -> None:
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = None
        self.buffered_updates.clear()
        self.sync_state = OrderBookSyncState.UNSYNCED
        self.sequence_integrity = reason

    def _snapshot_bridges_first_event(self, update: DepthUpdate) -> bool:
        if self.last_update_id is None:
            return False
        expected_update_id = self.last_update_id + 1
        return update.first_update_id <= expected_update_id <= update.final_update_id

    @staticmethod
    def _apply_levels(book: dict[float, float], levels: tuple[tuple[str, str], ...]) -> None:
        for price_raw, quantity_raw in levels:
            price = float(price_raw)
            quantity = float(quantity_raw)
            if quantity == 0:
                book.pop(price, None)
            else:
                book[price] = quantity


def _levels_to_book(levels: tuple[tuple[str, str], ...]) -> dict[float, float]:
    return {float(price): float(quantity) for price, quantity in levels if float(quantity) > 0}


def _book_to_levels(
    book: dict[float, float],
    levels: int,
    *,
    reverse: bool,
) -> tuple[tuple[str, str], ...]:
    prices = sorted(book.keys(), reverse=reverse)[:levels]
    return tuple((_format_float(price), _format_float(book[price])) for price in prices)


def _format_float(value: float) -> str:
    return f"{value:.12g}"
