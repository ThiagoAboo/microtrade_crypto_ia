from __future__ import annotations

import asyncio
import json
from collections import deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
import websockets

from core.config import MarketDataSettings
from market_data.clock import now_ms
from market_data.normalizers import normalize_rest_depth_snapshot
from market_data.models import RestDepthSnapshot


class BinanceRateLimitError(RuntimeError):
    def __init__(self, status_code: int, retry_after_seconds: float, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds


class RequestWeightLimiter:
    def __init__(self, weight_limit_per_minute: int) -> None:
        self._weight_limit_per_minute = weight_limit_per_minute
        self._events: deque[tuple[int, int]] = deque()
        self._lock = asyncio.Lock()
        self._cooldown_until_ms = 0

    async def acquire(self, weight: int) -> None:
        async with self._lock:
            while True:
                current_ms = now_ms()
                self._remove_expired(current_ms)
                cooldown_sleep_ms = self._cooldown_until_ms - current_ms
                used_weight = sum(item_weight for _, item_weight in self._events)
                limit_sleep_ms = 0
                if used_weight + weight > self._weight_limit_per_minute and self._events:
                    limit_sleep_ms = 60_000 - (current_ms - self._events[0][0])

                sleep_ms = max(cooldown_sleep_ms, limit_sleep_ms)
                if sleep_ms <= 0:
                    self._events.append((current_ms, weight))
                    return
                await asyncio.sleep(sleep_ms / 1000)

    def apply_cooldown(self, retry_after_seconds: float) -> None:
        self._cooldown_until_ms = max(
            self._cooldown_until_ms,
            now_ms() + int(retry_after_seconds * 1000),
        )

    def _remove_expired(self, current_ms: int) -> None:
        while self._events and current_ms - self._events[0][0] >= 60_000:
            self._events.popleft()


class BinanceSpotClient:
    def __init__(self, settings: MarketDataSettings) -> None:
        self._settings = settings
        self._http_client = httpx.AsyncClient(
            base_url=settings.rest_base_url,
            timeout=httpx.Timeout(10.0, connect=5.0),
            headers={"User-Agent": "microtrade-crypto-ia/0.1"},
        )
        self._rest_lock = asyncio.Lock()
        self._last_rest_call_ms = 0
        self._weight_limiter = RequestWeightLimiter(settings.rest_request_weight_limit_per_minute)

    def build_multiplex_url(self) -> str:
        streams: list[str] = []
        depth_suffix = "@depth@100ms" if self._settings.depth_stream_interval_ms == 100 else "@depth"
        for symbol in self._settings.symbols:
            lower_symbol = symbol.lower()
            streams.append(f"{lower_symbol}@trade")
            streams.append(f"{lower_symbol}{depth_suffix}")
        return f"{self._settings.websocket_base_url}?streams={'/'.join(streams)}"

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[Any]:
        async with websockets.connect(
            self.build_multiplex_url(),
            ping_interval=None,
            max_queue=32,
            open_timeout=10,
            close_timeout=5,
            compression=None,
        ) as websocket:
            yield websocket

    async def recv_combined_payload(self, websocket: Any) -> dict[str, Any]:
        raw_message = await asyncio.wait_for(
            websocket.recv(),
            timeout=self._settings.websocket_message_timeout_seconds,
        )
        message = json.loads(raw_message)
        if "data" not in message:
            raise ValueError("Binance combined stream message does not contain data")
        return dict(message["data"])

    async def fetch_depth_snapshot(self, symbol: str) -> RestDepthSnapshot:
        weight = _depth_snapshot_weight(self._settings.snapshot_limit)
        async with self._rest_lock:
            await self._respect_rest_spacing()
            await self._weight_limiter.acquire(weight)
            received_ts = now_ms()
            response = await self._http_client.get(
                "/api/v3/depth",
                params={"symbol": symbol.upper(), "limit": self._settings.snapshot_limit},
            )
            self._last_rest_call_ms = now_ms()
            if response.status_code in {429, 418}:
                retry_after_seconds = self._retry_after_seconds(response)
                self._weight_limiter.apply_cooldown(retry_after_seconds)
                raise BinanceRateLimitError(
                    response.status_code,
                    retry_after_seconds,
                    f"Binance REST rate limited with HTTP {response.status_code}",
                )
            response.raise_for_status()
            return normalize_rest_depth_snapshot(symbol, response.json(), received_ts)

    async def close(self) -> None:
        await self._http_client.aclose()

    async def _respect_rest_spacing(self) -> None:
        elapsed_ms = now_ms() - self._last_rest_call_ms
        remaining_ms = self._settings.rest_snapshot_min_interval_ms - elapsed_ms
        if remaining_ms > 0:
            await asyncio.sleep(remaining_ms / 1000)

    def _retry_after_seconds(self, response: httpx.Response) -> float:
        raw_retry_after = response.headers.get("Retry-After")
        fallback = min(
            self._settings.reconnect_max_delay_seconds,
            self._settings.rest_retry_after_max_seconds,
        )
        if raw_retry_after is None:
            return float(fallback)
        try:
            parsed = float(raw_retry_after)
        except ValueError:
            return float(fallback)
        return float(min(max(parsed, 1.0), self._settings.rest_retry_after_max_seconds))


def _depth_snapshot_weight(limit: int) -> int:
    if limit <= 100:
        return 5
    if limit <= 500:
        return 25
    if limit <= 1000:
        return 50
    return 250
