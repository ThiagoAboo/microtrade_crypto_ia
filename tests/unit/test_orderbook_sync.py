from market_data.models import ApplyUpdateResult, DepthUpdate, RestDepthSnapshot
from market_data.orderbook import LocalOrderBook


def test_orderbook_initializes_from_snapshot_and_buffered_updates() -> None:
    book = LocalOrderBook("BTCUSDT", levels=10, max_buffered_updates=10)
    book.start_buffering()
    book.buffer_update(_depth_update(first=100, final=105, bid_price="99", bid_qty="2"))

    result = book.initialize_from_snapshot(
        RestDepthSnapshot(
            received_ts=1,
            processed_ts=2,
            symbol="BTCUSDT",
            source_exchange="binance_spot",
            last_update_id=100,
            bids=(("98", "1"),),
            asks=(("101", "1"),),
        )
    )

    assert result == ApplyUpdateResult.APPLIED
    assert book.last_update_id == 105
    assert book.top_bids()[0] == ("99", "2")


def test_orderbook_detects_gap_after_sync() -> None:
    book = LocalOrderBook("BTCUSDT", levels=10, max_buffered_updates=10)
    book.start_buffering()
    book.buffer_update(_depth_update(first=100, final=105, bid_price="99", bid_qty="2"))
    book.initialize_from_snapshot(
        RestDepthSnapshot(
            received_ts=1,
            processed_ts=2,
            symbol="BTCUSDT",
            source_exchange="binance_spot",
            last_update_id=100,
            bids=(("98", "1"),),
            asks=(("101", "1"),),
        )
    )

    result = book.apply_update(_depth_update(first=110, final=111, bid_price="100", bid_qty="1"))

    assert result == ApplyUpdateResult.GAP_DETECTED
    assert book.last_update_id is None


def test_orderbook_rejects_invalid_sequence() -> None:
    book = LocalOrderBook("BTCUSDT", levels=10, max_buffered_updates=10)
    book.start_buffering()
    book.buffer_update(_depth_update(first=100, final=105, bid_price="99", bid_qty="2"))
    book.initialize_from_snapshot(
        RestDepthSnapshot(
            received_ts=1,
            processed_ts=2,
            symbol="BTCUSDT",
            source_exchange="binance_spot",
            last_update_id=100,
            bids=(("98", "1"),),
            asks=(("101", "1"),),
        )
    )

    result = book.apply_update(_depth_update(first=110, final=109, bid_price="100", bid_qty="1"))

    assert result == ApplyUpdateResult.INVALID_SEQUENCE


def test_orderbook_ignores_duplicate_update() -> None:
    book = _synced_book(last_update_id=105)

    result = book.apply_update(_depth_update(first=100, final=105, bid_price="99", bid_qty="9"))

    assert result == ApplyUpdateResult.IGNORED_OLD
    assert book.last_update_id == 105
    assert book.top_bids()[0] == ("98", "1")


def test_orderbook_detects_reordered_future_update_as_gap() -> None:
    book = _synced_book(last_update_id=105)

    result = book.apply_update(_depth_update(first=107, final=108, bid_price="99", bid_qty="2"))

    assert result == ApplyUpdateResult.GAP_DETECTED
    assert book.last_update_id is None


def test_orderbook_keeps_buffer_when_snapshot_is_stale() -> None:
    book = LocalOrderBook("BTCUSDT", levels=10, max_buffered_updates=10)
    book.start_buffering()
    book.buffer_update(_depth_update(first=100, final=105, bid_price="99", bid_qty="2"))

    result = book.initialize_from_snapshot(
        RestDepthSnapshot(
            received_ts=1,
            processed_ts=2,
            symbol="BTCUSDT",
            source_exchange="binance_spot",
            last_update_id=99,
            bids=(("98", "1"),),
            asks=(("101", "1"),),
        )
    )

    assert result == ApplyUpdateResult.IGNORED_OLD
    assert len(book.buffered_updates) == 1


def test_orderbook_rejects_snapshot_without_valid_bridge() -> None:
    book = LocalOrderBook("BTCUSDT", levels=10, max_buffered_updates=10)
    book.start_buffering()
    book.buffer_update(_depth_update(first=100, final=105, bid_price="99", bid_qty="2"))
    book.buffer_update(_depth_update(first=110, final=115, bid_price="100", bid_qty="2"))

    result = book.initialize_from_snapshot(
        RestDepthSnapshot(
            received_ts=1,
            processed_ts=2,
            symbol="BTCUSDT",
            source_exchange="binance_spot",
            last_update_id=107,
            bids=(("98", "1"),),
            asks=(("101", "1"),),
        )
    )

    assert result == ApplyUpdateResult.GAP_DETECTED
    assert book.last_update_id is None


def test_orderbook_buffer_overflow_invalidates_book() -> None:
    book = LocalOrderBook("BTCUSDT", levels=10, max_buffered_updates=1)
    book.start_buffering()

    assert book.apply_update(_depth_update(first=100, final=100, bid_price="99", bid_qty="1"))
    result = book.apply_update(_depth_update(first=101, final=101, bid_price="100", bid_qty="1"))

    assert result == ApplyUpdateResult.GAP_DETECTED
    assert book.last_update_id is None


def _synced_book(last_update_id: int) -> LocalOrderBook:
    book = LocalOrderBook("BTCUSDT", levels=10, max_buffered_updates=10)
    book.initialize_from_snapshot(
        RestDepthSnapshot(
            received_ts=1,
            processed_ts=2,
            symbol="BTCUSDT",
            source_exchange="binance_spot",
            last_update_id=last_update_id,
            bids=(("98", "1"),),
            asks=(("101", "1"),),
        )
    )
    return book


def _depth_update(first: int, final: int, bid_price: str, bid_qty: str) -> DepthUpdate:
    return DepthUpdate(
        exchange_ts=1000,
        received_ts=1001,
        processed_ts=1002,
        symbol="BTCUSDT",
        source_exchange="binance_spot",
        first_update_id=first,
        final_update_id=final,
        bids=((bid_price, bid_qty),),
        asks=(),
        ingest_latency_ms=2,
    )
