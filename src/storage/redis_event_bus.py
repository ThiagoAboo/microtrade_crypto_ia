from __future__ import annotations

import asyncio
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import RedisError, ResponseError

from core.config import RedisSettings, RetrySettings
from core.event_bus import StreamMessage
from core.events import EventEnvelope, EventType
from core.logging import get_logger
from core.retry import RetryPolicy, retry_async
from core.streams import SYSTEM_ALERTS_STREAM


class RedisStreamEventBus:
    def __init__(self, settings: RedisSettings, retry_settings: RetrySettings) -> None:
        self._settings = settings
        self._retry_policy = RetryPolicy(
            max_attempts=retry_settings.max_attempts,
            base_delay_seconds=retry_settings.base_delay_seconds,
            max_delay_seconds=retry_settings.max_delay_seconds,
            backoff_multiplier=retry_settings.backoff_multiplier,
        )
        timeout_seconds = settings.socket_timeout_ms / 1000
        self._client: Redis = Redis.from_url(
            settings.url,
            decode_responses=False,
            socket_connect_timeout=timeout_seconds,
            socket_timeout=timeout_seconds,
        )
        self._logger = get_logger(__name__)

    async def ensure_streams(self, streams: tuple[str, ...]) -> None:
        for stream in streams:
            await retry_async(
                lambda stream_name=stream: self._ensure_stream(stream_name),
                self._retry_policy,
                operation_name=f"redis.ensure_stream.{stream}",
                logger=self._logger,
                retry_exceptions=(RedisError,),
            )

    async def ping(self) -> bool:
        return bool(
            await retry_async(
                lambda: self._bounded_ping(),
                self._retry_policy,
                operation_name="redis.ping",
                logger=self._logger,
                retry_exceptions=(RedisError, TimeoutError),
            )
        )

    async def publish(self, stream: str, event: EventEnvelope) -> str:
        self._validate_stream(stream)
        message_id = await retry_async(
            lambda: self._client.xadd(
                stream,
                event.to_stream_fields(),
                maxlen=self._settings.stream_max_len,
                approximate=True,
            ),
            self._retry_policy,
            operation_name=f"redis.publish.{stream}",
            logger=self._logger,
            retry_exceptions=(RedisError,),
        )
        return _decode_redis_value(message_id)

    async def read(
        self,
        stream: str,
        group: str,
        consumer: str,
        count: int = 10,
    ) -> list[StreamMessage]:
        if count < 1:
            raise ValueError("count must be greater than zero")
        self._validate_stream(stream)
        await retry_async(
            lambda: self._ensure_consumer_group(stream, group),
            self._retry_policy,
            operation_name=f"redis.ensure_consumer_group.{stream}",
            logger=self._logger,
            retry_exceptions=(RedisError,),
        )

        messages = await self._claim_pending(stream, group, consumer, count)
        if len(messages) >= count:
            return messages

        response = await retry_async(
            lambda: self._client.xreadgroup(
                group,
                consumer,
                streams={stream: ">"},
                count=count - len(messages),
                block=self._settings.consumer_block_ms,
            ),
            self._retry_policy,
            operation_name=f"redis.read.{stream}",
            logger=self._logger,
            retry_exceptions=(RedisError,),
        )

        for raw_stream, raw_messages in response:
            stream_name = _decode_redis_value(raw_stream)
            messages.extend(
                await self._decode_messages_or_dead_letter(
                    stream=stream_name,
                    group=group,
                    raw_messages=raw_messages,
                )
            )
        return messages

    async def ack(self, stream: str, group: str, message_id: str) -> int:
        self._validate_stream(stream)
        return int(
            await retry_async(
                lambda: self._client.xack(stream, group, message_id),
                self._retry_policy,
                operation_name=f"redis.ack.{stream}",
                logger=self._logger,
                retry_exceptions=(RedisError,),
            )
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def _bounded_ping(self) -> bool:
        return bool(
            await asyncio.wait_for(
                self._client.ping(),
                timeout=self._settings.health_timeout_ms / 1000,
            )
        )

    async def _ensure_stream(self, stream: str) -> None:
        self._validate_stream(stream)
        await self._ensure_consumer_group(stream, self._settings.bootstrap_consumer_group)

    async def _ensure_consumer_group(self, stream: str, group: str) -> None:
        try:
            await self._client.xgroup_create(stream, group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _claim_pending(
        self,
        stream: str,
        group: str,
        consumer: str,
        count: int,
    ) -> list[StreamMessage]:
        response = await retry_async(
            lambda: self._client.xautoclaim(
                stream,
                group,
                consumer,
                min_idle_time=self._settings.pending_idle_ms,
                start_id="0-0",
                count=count,
            ),
            self._retry_policy,
            operation_name=f"redis.claim_pending.{stream}",
            logger=self._logger,
            retry_exceptions=(RedisError,),
        )
        raw_messages = response[1] if len(response) >= 2 else []
        return await self._decode_messages_or_dead_letter(
            stream=stream,
            group=group,
            raw_messages=raw_messages,
        )

    async def _decode_messages_or_dead_letter(
        self,
        *,
        stream: str,
        group: str,
        raw_messages: list[tuple[Any, dict[Any, Any]]],
    ) -> list[StreamMessage]:
        messages: list[StreamMessage] = []
        for raw_message_id, raw_fields in raw_messages:
            message_id = _decode_redis_value(raw_message_id)
            try:
                event = EventEnvelope.from_stream_fields(raw_fields)
            except Exception as exc:
                await self._dead_letter_poison_message(
                    stream,
                    group,
                    message_id,
                    raw_fields,
                    exc,
                )
                continue
            messages.append(StreamMessage(stream=stream, message_id=message_id, event=event))
        return messages

    async def _dead_letter_poison_message(
        self,
        stream: str,
        group: str,
        message_id: str,
        raw_fields: dict[Any, Any],
        exc: Exception,
    ) -> None:
        alert = EventEnvelope(
            event_type=EventType.SYSTEM_ALERT,
            source="redis_event_bus",
            payload={
                "reason": "poison_message",
                "original_stream": stream,
                "original_message_id": message_id,
                "error": str(exc),
                "fields": _sanitize_fields(
                    raw_fields,
                    self._settings.dead_letter_max_field_chars,
                ),
            },
        )
        try:
            await self.publish(SYSTEM_ALERTS_STREAM, alert)
        except Exception:
            self._logger.error(
                "failed to publish poison Redis stream message alert",
                extra={
                    "category": "ERROR",
                    "stream": stream,
                    "message_id": message_id,
                },
                exc_info=True,
            )
        await self.ack(stream, group, message_id)
        self._logger.warning(
            "poison Redis stream message moved to system alerts",
            extra={
                "category": "SYSTEM",
                "stream": stream,
                "message_id": message_id,
            },
        )

    def _validate_stream(self, stream: str) -> None:
        if stream not in self._settings.streams:
            raise ValueError(f"unsupported Redis stream for Phase 1: {stream}")


def _decode_redis_value(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _sanitize_fields(fields: dict[Any, Any], max_chars: int) -> dict[str, str]:
    sanitized: dict[str, str] = {}
    for key, value in fields.items():
        decoded_key = _decode_redis_value(key)
        decoded_value = _decode_redis_value(value)
        sanitized[decoded_key] = decoded_value[:max_chars]
    return sanitized
