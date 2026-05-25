import pytest
from pydantic import ValidationError

from core.events import EventEnvelope, EventType


def test_event_envelope_round_trips_for_redis_streams() -> None:
    event = EventEnvelope(
        event_type=EventType.MARKET_TICK,
        source="unit-test",
        symbol="btcusdt",
        payload={"price": 100.5, "quantity": 0.2},
    )

    restored = EventEnvelope.from_stream_fields(event.to_stream_fields())

    assert restored.event_id == event.event_id
    assert restored.event_type == EventType.MARKET_TICK
    assert restored.symbol == "BTCUSDT"
    assert restored.payload["price"] == 100.5


def test_event_envelope_rejects_blank_source() -> None:
    with pytest.raises(ValidationError):
        EventEnvelope(event_type=EventType.SYSTEM_ALERT, source="   ")
