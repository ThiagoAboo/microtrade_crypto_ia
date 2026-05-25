import asyncio

from core.config import RedisSettings, RetrySettings
from core.streams import SYSTEM_ALERTS_STREAM
from storage.redis_event_bus import RedisStreamEventBus


class FakeRedisClient:
    def __init__(self) -> None:
        self.added: list[tuple[str, dict[str, str]]] = []
        self.acked: list[tuple[str, str, str]] = []

    async def xadd(
        self,
        stream: str,
        fields: dict[str, str],
        maxlen: int,
        approximate: bool,
    ) -> bytes:
        self.added.append((stream, fields))
        return b"1-0"

    async def xack(self, stream: str, group: str, message_id: str) -> int:
        self.acked.append((stream, group, message_id))
        return 1


def test_poison_message_is_moved_to_system_alerts() -> None:
    async def run_test() -> None:
        fake_client = FakeRedisClient()
        event_bus = RedisStreamEventBus(RedisSettings(), RetrySettings())
        event_bus._client = fake_client  # type: ignore[assignment]

        messages = await event_bus._decode_messages_or_dead_letter(
            stream="market:ticks",
            group="unit-test",
            raw_messages=[(b"2-0", {b"not_event": b"bad-payload"})],
        )

        assert messages == []
        assert fake_client.added[0][0] == SYSTEM_ALERTS_STREAM
        assert fake_client.acked == [("market:ticks", "unit-test", "2-0")]

    asyncio.run(run_test())

