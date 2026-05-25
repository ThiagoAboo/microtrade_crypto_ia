import pytest
import httpx

from core.config import MarketDataSettings
from market_data.binance_client import BinanceRateLimitError, BinanceSpotClient


@pytest.mark.anyio
async def test_fetch_depth_snapshot_raises_rate_limit_with_retry_after() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "2"}, json={"code": -1003})

    client = BinanceSpotClient(MarketDataSettings(symbols=("BTCUSDT",)))
    await client._http_client.aclose()
    client._http_client = httpx.AsyncClient(
        base_url="https://api.binance.test",
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(BinanceRateLimitError) as exc_info:
        await client.fetch_depth_snapshot("BTCUSDT")

    assert exc_info.value.status_code == 429
    assert exc_info.value.retry_after_seconds == 2

    await client.close()


@pytest.mark.anyio
async def test_fetch_depth_snapshot_caps_retry_after() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(418, headers={"Retry-After": "9999"}, json={"code": -1003})

    settings = MarketDataSettings(symbols=("BTCUSDT",), rest_retry_after_max_seconds=30)
    client = BinanceSpotClient(settings)
    await client._http_client.aclose()
    client._http_client = httpx.AsyncClient(
        base_url="https://api.binance.test",
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(BinanceRateLimitError) as exc_info:
        await client.fetch_depth_snapshot("BTCUSDT")

    assert exc_info.value.status_code == 418
    assert exc_info.value.retry_after_seconds == 30

    await client.close()
