from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

from core.config import ClickHouseSettings, RetrySettings
from core.logging import get_logger
from core.retry import RetryPolicy, retry_async


class ClickHouseClient:
    def __init__(self, settings: ClickHouseSettings, retry_settings: RetrySettings) -> None:
        self._settings = settings
        self._retry_policy = RetryPolicy(
            max_attempts=retry_settings.max_attempts,
            base_delay_seconds=retry_settings.base_delay_seconds,
            max_delay_seconds=retry_settings.max_delay_seconds,
            backoff_multiplier=retry_settings.backoff_multiplier,
        )
        self._client: Client | None = None
        self._lock = asyncio.Lock()
        self._logger = get_logger(__name__)

    async def ping(self) -> bool:
        async with self._lock:
            return bool(
                await retry_async(
                    lambda: asyncio.to_thread(self._ping_once),
                    self._retry_policy,
                    operation_name="clickhouse.ping",
                    logger=self._logger,
                )
            )

    async def close(self) -> None:
        async with self._lock:
            if self._client is not None:
                await asyncio.to_thread(self._client.close)
                self._client = None

    def _ping_once(self) -> bool:
        result = self._get_client().command("SELECT 1")
        return str(result).strip() == "1"

    def _get_client(self) -> Client:
        if self._client is None:
            import clickhouse_connect

            self._client = clickhouse_connect.get_client(
                host=self._settings.host,
                port=self._settings.port,
                username=self._settings.username,
                password=self._settings.password,
                database=self._settings.database,
                connect_timeout=self._settings.connect_timeout_seconds,
                send_receive_timeout=self._settings.send_receive_timeout_seconds,
                autogenerate_session_id=False,
            )
        return self._client
