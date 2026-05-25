from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from core.events import EventEnvelope


@dataclass(frozen=True, slots=True)
class StreamMessage:
    stream: str
    message_id: str
    event: EventEnvelope


class EventBus(Protocol):
    async def ensure_streams(self, streams: tuple[str, ...]) -> None:
        ...

    async def ping(self) -> bool:
        ...

    async def publish(self, stream: str, event: EventEnvelope) -> str:
        ...

    async def read(
        self,
        stream: str,
        group: str,
        consumer: str,
        count: int = 10,
    ) -> list[StreamMessage]:
        ...

    async def ack(self, stream: str, group: str, message_id: str) -> int:
        ...

    async def close(self) -> None:
        ...

