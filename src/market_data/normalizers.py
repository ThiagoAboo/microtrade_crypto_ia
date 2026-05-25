from __future__ import annotations

from typing import Any

from market_data.clock import latency_ms, now_ms
from market_data.models import DepthUpdate, NormalizedTrade, RestDepthSnapshot, SOURCE_EXCHANGE


def normalize_trade(payload: dict[str, Any], received_ts: int | None = None) -> NormalizedTrade:
    received = received_ts or now_ms()
    processed = now_ms()
    symbol = str(payload["s"]).upper()
    trade_id = int(payload["t"])
    exchange_ts = int(payload.get("T") or payload["E"])
    is_buyer_maker = bool(payload["m"])

    return NormalizedTrade(
        exchange_ts=exchange_ts,
        received_ts=received,
        processed_ts=processed,
        symbol=symbol,
        source_exchange=SOURCE_EXCHANGE,
        sequence=trade_id,
        trade_id=str(trade_id),
        price=float(payload["p"]),
        quantity=float(payload["q"]),
        side="sell" if is_buyer_maker else "buy",
        is_buyer_maker=is_buyer_maker,
        ingest_latency_ms=latency_ms(exchange_ts, processed),
    )


def normalize_depth_update(payload: dict[str, Any], received_ts: int | None = None) -> DepthUpdate:
    received = received_ts or now_ms()
    processed = now_ms()
    exchange_ts = int(payload["E"])

    return DepthUpdate(
        exchange_ts=exchange_ts,
        received_ts=received,
        processed_ts=processed,
        symbol=str(payload["s"]).upper(),
        source_exchange=SOURCE_EXCHANGE,
        first_update_id=int(payload["U"]),
        final_update_id=int(payload["u"]),
        bids=_normalize_levels(payload.get("b", [])),
        asks=_normalize_levels(payload.get("a", [])),
        ingest_latency_ms=latency_ms(exchange_ts, processed),
    )


def normalize_rest_depth_snapshot(
    symbol: str,
    payload: dict[str, Any],
    received_ts: int | None = None,
) -> RestDepthSnapshot:
    received = received_ts or now_ms()
    processed = now_ms()
    return RestDepthSnapshot(
        received_ts=received,
        processed_ts=processed,
        symbol=symbol.upper(),
        source_exchange=SOURCE_EXCHANGE,
        last_update_id=int(payload["lastUpdateId"]),
        bids=_normalize_levels(payload.get("bids", [])),
        asks=_normalize_levels(payload.get("asks", [])),
    )


def _normalize_levels(raw_levels: list[list[str]]) -> tuple[tuple[str, str], ...]:
    return tuple((str(price), str(quantity)) for price, quantity in raw_levels)

