from __future__ import annotations

import time
from enum import StrEnum
from typing import Any, Mapping
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


class EventType(StrEnum):
    MARKET_TICK = "MARKET_TICK"
    ORDERBOOK_UPDATE = "ORDERBOOK_UPDATE"
    SYSTEM_ALERT = "SYSTEM_ALERT"


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: UUID = Field(default_factory=uuid4)
    event_type: EventType
    timestamp: int = Field(default_factory=lambda: int(time.time() * 1000), ge=0)
    source: str = Field(min_length=1)
    symbol: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, symbol: str | None) -> str | None:
        if symbol is None:
            return None
        normalized = symbol.strip().upper()
        return normalized or None

    @field_validator("source")
    @classmethod
    def normalize_source(cls, source: str) -> str:
        normalized = source.strip()
        if not normalized:
            raise ValueError("source cannot be blank")
        return normalized

    def to_stream_fields(self) -> dict[str, str]:
        return {"event": self.model_dump_json()}

    @classmethod
    def from_stream_fields(cls, fields: Mapping[str | bytes, str | bytes]) -> EventEnvelope:
        raw_event = fields.get("event") or fields.get(b"event")
        if raw_event is None:
            raise ValueError("Redis stream message does not contain an 'event' field")
        if isinstance(raw_event, bytes):
            raw_event = raw_event.decode("utf-8")
        return cls.model_validate_json(raw_event)
