import asyncio

from core.config import MarketDataSettings
from market_data.backpressure import MarketDataQueues
from market_data.models import (
    NormalizedTrade,
    OrderBookSyncState,
    ReducedOrderBookSnapshot,
    SequenceIntegrityState,
)


def test_snapshot_queue_drops_oldest_snapshot_only() -> None:
    queues = MarketDataQueues(
        MarketDataSettings(symbols=("BTCUSDT",), snapshot_queue_max_size_per_symbol=1)
    )

    first = _snapshot(sequence=1)
    second = _snapshot(sequence=2)

    assert queues.put_snapshot(first) is False
    assert queues.put_snapshot(second) is True

    drained = queues.drain_snapshots("BTCUSDT", 10)
    assert [snapshot.sequence for snapshot in drained] == [2]


def test_trade_queue_blocks_instead_of_dropping() -> None:
    async def run_test() -> None:
        queues = MarketDataQueues(MarketDataSettings(symbols=("BTCUSDT",), queue_max_size_per_symbol=100))
        trade = NormalizedTrade(
            exchange_ts=1,
            received_ts=2,
            processed_ts=3,
            symbol="BTCUSDT",
            source_exchange="binance_spot",
            sequence=1,
            trade_id="1",
            price=1.0,
            quantity=1.0,
            side="buy",
            is_buyer_maker=False,
            ingest_latency_ms=2.0,
        )

        result = await queues.put_trade(trade)

        assert result.accepted is True
        assert result.had_backpressure is False
        assert queues.drain_trades("BTCUSDT", 10) == [trade]

    asyncio.run(run_test())


def test_trade_queue_drops_oldest_when_full() -> None:
    async def run_test() -> None:
        queues = MarketDataQueues(
            MarketDataSettings(
                symbols=("BTCUSDT",),
                queue_max_size_per_symbol=100,
                trade_queue_put_timeout_ms=1,
            )
        )
        for sequence in range(100):
            result = await queues.put_trade(_trade(sequence))
            assert result.accepted is True

        overflow_result = await queues.put_trade(_trade(100))

        assert overflow_result.accepted is True
        assert overflow_result.had_backpressure is True
        assert overflow_result.dropped_trade is True
        assert overflow_result.reason == "drop_oldest"
        drained = queues.drain_trades("BTCUSDT", 200)
        assert [trade.sequence for trade in drained][:2] == [1, 2]
        assert drained[-1].sequence == 100
        assert queues.lag_for_symbol("BTCUSDT").dropped_trades == 1

    asyncio.run(run_test())


def test_requeue_trades_drops_when_queue_is_full_without_blocking() -> None:
    queues = MarketDataQueues(MarketDataSettings(symbols=("BTCUSDT",), queue_max_size_per_symbol=100))
    dropped = queues.requeue_trades([_trade(sequence) for sequence in range(101)])

    assert dropped == 1
    assert queues.lag_for_symbol("BTCUSDT").dropped_requeued_trades == 1


def _snapshot(sequence: int) -> ReducedOrderBookSnapshot:
    return ReducedOrderBookSnapshot(
        exchange_ts=1,
        received_ts=2,
        processed_ts=3,
        symbol="BTCUSDT",
        source_exchange="binance_spot",
        sequence=sequence,
        first_update_id=sequence,
        final_update_id=sequence,
        last_update_id=sequence,
        best_bid=1.0,
        best_ask=2.0,
        spread=1.0,
        bid_levels=(("1", "1"),),
        ask_levels=(("2", "1"),),
        levels=10,
        sync_state=OrderBookSyncState.SYNCED,
        sequence_integrity=SequenceIntegrityState.OK,
        ingest_latency_ms=1.0,
    )


def _trade(sequence: int) -> NormalizedTrade:
    return NormalizedTrade(
        exchange_ts=1,
        received_ts=2,
        processed_ts=3,
        symbol="BTCUSDT",
        source_exchange="binance_spot",
        sequence=sequence,
        trade_id=str(sequence),
        price=1.0,
        quantity=1.0,
        side="buy",
        is_buyer_maker=False,
        ingest_latency_ms=2.0,
    )
