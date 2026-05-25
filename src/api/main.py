from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Protocol

from fastapi import FastAPI, Response, status

from core.config import AppSettings, load_settings
from core.event_bus import EventBus
from core.health import DependencyHealth, HealthResponse
from core.logging import LogCategory, configure_logging, get_logger
from market_data.engine import MarketDataEngine
from storage.clickhouse import ClickHouseClient
from storage.redis_event_bus import RedisStreamEventBus


class HealthDependency(Protocol):
    async def ping(self) -> bool:
        ...

    async def close(self) -> None:
        ...


class RedisDependency(EventBus, Protocol):
    pass


class MarketDataDependency(Protocol):
    async def ping(self) -> bool:
        ...

    def health_snapshot(self) -> dict[str, object]:
        ...

    async def start(self) -> None:
        ...

    async def stop(self) -> None:
        ...


def create_app(
    settings: AppSettings | None = None,
    redis_event_bus: RedisDependency | None = None,
    clickhouse_client: HealthDependency | None = None,
    market_data_engine: MarketDataDependency | None = None,
) -> FastAPI:
    resolved_settings = settings or load_settings()
    configure_logging(
        level=resolved_settings.logging.level,
        json_logs=resolved_settings.logging.json_logs,
    )
    logger = get_logger(__name__)
    owns_redis = redis_event_bus is None
    owns_clickhouse = clickhouse_client is None
    owns_market_data = market_data_engine is None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        redis_dependency = redis_event_bus or RedisStreamEventBus(
            resolved_settings.redis,
            resolved_settings.retry,
        )
        clickhouse_dependency = clickhouse_client or ClickHouseClient(
            resolved_settings.clickhouse,
            resolved_settings.retry,
        )

        app.state.settings = resolved_settings
        app.state.redis_event_bus = redis_dependency
        app.state.clickhouse_client = clickhouse_dependency

        try:
            await redis_dependency.ensure_streams(resolved_settings.redis.streams)
            logger.info(
                "Redis streams are ready",
                extra={"category": LogCategory.SYSTEM.value},
            )
        except Exception:
            logger.warning(
                "Redis streams are not ready yet",
                extra={"category": LogCategory.SYSTEM.value},
                exc_info=True,
            )

        market_data_dependency = market_data_engine
        if resolved_settings.market_data.enabled and market_data_dependency is None:
            market_data_dependency = MarketDataEngine(resolved_settings, redis_dependency)
        app.state.market_data_engine = market_data_dependency
        if market_data_dependency is not None:
            try:
                await market_data_dependency.start()
                logger.info(
                    "Market data engine start requested",
                    extra={"category": LogCategory.SYSTEM.value},
                )
            except Exception:
                logger.warning(
                    "Market data engine did not start cleanly",
                    extra={"category": LogCategory.SYSTEM.value},
                    exc_info=True,
                )

        yield

        if owns_market_data and app.state.market_data_engine is not None:
            try:
                await app.state.market_data_engine.stop()
            except Exception:
                logger.warning(
                    "Market data engine did not stop cleanly",
                    extra={"category": LogCategory.SYSTEM.value},
                    exc_info=True,
                )
        if owns_redis:
            try:
                await redis_dependency.close()
            except Exception:
                logger.warning(
                    "Redis dependency did not close cleanly",
                    extra={"category": LogCategory.SYSTEM.value},
                    exc_info=True,
                )
        if owns_clickhouse:
            try:
                await clickhouse_dependency.close()
            except Exception:
                logger.warning(
                    "ClickHouse dependency did not close cleanly",
                    extra={"category": LogCategory.SYSTEM.value},
                    exc_info=True,
                )

    app = FastAPI(
        title=resolved_settings.app_name,
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.get("/health/live", response_model=HealthResponse)
    async def live() -> HealthResponse:
        return HealthResponse(
            status="ok",
            service=resolved_settings.app_name,
            environment=resolved_settings.environment,
            dependencies=[],
            latency_ms=0.0,
        )

    @app.get("/health/ready", response_model=HealthResponse)
    async def ready(response: Response) -> HealthResponse:
        started_at = time.perf_counter()
        dependency_checks = [
            _measure_dependency("redis", app.state.redis_event_bus.ping),
            _measure_dependency("clickhouse", app.state.clickhouse_client.ping),
        ]
        if app.state.market_data_engine is not None:
            dependency_checks.append(
                _measure_dependency("market_data", app.state.market_data_engine.ping)
            )
        dependencies = await asyncio.gather(*dependency_checks)
        response_status = "ok" if all(item.status == "ok" for item in dependencies) else "unhealthy"
        latency_ms = _elapsed_ms(started_at)

        if response_status != "ok":
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

        logger.info(
            "readiness check completed",
            extra={
                "category": LogCategory.SYSTEM.value,
                "latency_ms": latency_ms,
                "status": response_status,
            },
        )

        return HealthResponse(
            status=response_status,
            service=resolved_settings.app_name,
            environment=resolved_settings.environment,
            dependencies=dependencies,
            latency_ms=latency_ms,
        )

    @app.get("/health/market-data")
    async def market_data_health() -> dict[str, object]:
        if app.state.market_data_engine is None:
            return {"enabled": False, "status": "disabled"}
        return app.state.market_data_engine.health_snapshot()

    return app


async def _measure_dependency(
    name: str,
    check: Callable[[], Awaitable[bool]],
) -> DependencyHealth:
    started_at = time.perf_counter()
    try:
        healthy = await check()
        return DependencyHealth(
            name=name,
            status="ok" if healthy else "unhealthy",
            latency_ms=_elapsed_ms(started_at),
            error=None if healthy else "dependency returned an unhealthy status",
        )
    except Exception as exc:
        return DependencyHealth(
            name=name,
            status="unhealthy",
            latency_ms=_elapsed_ms(started_at),
            error=str(exc),
        )


def _elapsed_ms(started_at: float) -> float:
    return round((time.perf_counter() - started_at) * 1000, 3)
