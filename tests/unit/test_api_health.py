from fastapi.testclient import TestClient

from api.main import create_app
from core.config import AppSettings, LoggingSettings, MarketDataSettings


class FakeRedis:
    def __init__(self, healthy: bool = True) -> None:
        self.healthy = healthy
        self.streams: tuple[str, ...] | None = None
        self.closed = False

    async def ensure_streams(self, streams: tuple[str, ...]) -> None:
        self.streams = streams

    async def ping(self) -> bool:
        return self.healthy

    async def close(self) -> None:
        self.closed = True


class FakeDependency:
    def __init__(self, healthy: bool = True) -> None:
        self.healthy = healthy
        self.closed = False

    async def ping(self) -> bool:
        return self.healthy

    async def close(self) -> None:
        self.closed = True


class FakeMarketDataEngine:
    def __init__(self, healthy: bool = True, fail_start: bool = False) -> None:
        self.healthy = healthy
        self.fail_start = fail_start
        self.started = False
        self.stopped = False

    async def ping(self) -> bool:
        return self.healthy

    def health_snapshot(self) -> dict[str, object]:
        return {
            "enabled": True,
            "status": "running" if self.healthy else "degraded",
            "symbols": {"BTCUSDT": {"orderbook_sync_state": "synced"}},
        }

    async def start(self) -> None:
        if self.fail_start:
            raise RuntimeError("market data degraded at startup")
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


def test_ready_health_returns_ok_when_dependencies_are_healthy() -> None:
    settings = AppSettings(logging=LoggingSettings(json_logs=False))
    redis = FakeRedis()
    clickhouse = FakeDependency()
    app = create_app(settings, redis_event_bus=redis, clickhouse_client=clickhouse)

    with TestClient(app) as client:
        response = client.get("/health/ready")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert redis.streams == settings.redis.streams
    assert {item["name"] for item in payload["dependencies"]} == {"redis", "clickhouse"}


def test_ready_health_returns_503_when_dependency_is_unhealthy() -> None:
    settings = AppSettings(logging=LoggingSettings(json_logs=False))
    app = create_app(
        settings,
        redis_event_bus=FakeRedis(healthy=False),
        clickhouse_client=FakeDependency(),
    )

    with TestClient(app) as client:
        response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["status"] == "unhealthy"


def test_market_data_health_endpoint_uses_injected_engine() -> None:
    settings = AppSettings(
        logging=LoggingSettings(json_logs=False),
        market_data=MarketDataSettings(enabled=True, symbols=("BTCUSDT",)),
    )
    market_data = FakeMarketDataEngine()
    app = create_app(
        settings,
        redis_event_bus=FakeRedis(),
        clickhouse_client=FakeDependency(),
        market_data_engine=market_data,
    )

    with TestClient(app) as client:
        ready_response = client.get("/health/ready")
        market_data_response = client.get("/health/market-data")

    assert market_data.started is True
    assert ready_response.status_code == 200
    assert {item["name"] for item in ready_response.json()["dependencies"]} == {
        "redis",
        "clickhouse",
        "market_data",
    }
    assert market_data_response.status_code == 200
    assert market_data_response.json()["symbols"]["BTCUSDT"]["orderbook_sync_state"] == "synced"


def test_api_still_starts_when_market_data_start_fails() -> None:
    settings = AppSettings(
        logging=LoggingSettings(json_logs=False),
        market_data=MarketDataSettings(enabled=True, symbols=("BTCUSDT",)),
    )
    app = create_app(
        settings,
        redis_event_bus=FakeRedis(),
        clickhouse_client=FakeDependency(),
        market_data_engine=FakeMarketDataEngine(healthy=False, fail_start=True),
    )

    with TestClient(app) as client:
        response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["status"] == "unhealthy"
