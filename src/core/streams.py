"""Redis Stream names owned by Phase 1 infrastructure."""

MARKET_TICKS_STREAM = "market:ticks"
MARKET_ORDERBOOK_STREAM = "market:orderbook"
SYSTEM_ALERTS_STREAM = "system:alerts"

PHASE1_STREAMS: tuple[str, ...] = (
    MARKET_TICKS_STREAM,
    MARKET_ORDERBOOK_STREAM,
    SYSTEM_ALERTS_STREAM,
)

