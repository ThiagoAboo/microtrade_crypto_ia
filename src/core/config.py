from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Self

import yaml

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from core.streams import PHASE1_STREAMS, SYSTEM_ALERTS_STREAM


class ApiSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str = "0.0.0.0"
    port: int = Field(default=8000, ge=1, le=65535)


class RedisSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str = "redis://localhost:6379/0"
    stream_max_len: int = Field(default=10_000, ge=100)
    bootstrap_consumer_group: str = "bootstrap"
    consumer_block_ms: int = Field(default=1_000, ge=1)
    health_timeout_ms: int = Field(default=500, ge=50)
    socket_timeout_ms: int = Field(default=2_000, ge=100)
    pending_idle_ms: int = Field(default=30_000, ge=1_000)
    dead_letter_max_field_chars: int = Field(default=2_048, ge=128, le=8_192)
    streams: tuple[str, ...] = Field(default_factory=lambda: PHASE1_STREAMS)

    @field_validator("streams")
    @classmethod
    def validate_streams(cls, streams: tuple[str, ...]) -> tuple[str, ...]:
        if not streams:
            raise ValueError("at least one Redis stream must be configured")
        if len(set(streams)) != len(streams):
            raise ValueError("Redis streams must be unique")
        unsupported = sorted(set(streams) - set(PHASE1_STREAMS))
        if unsupported:
            raise ValueError(f"unsupported Phase 1 streams: {unsupported}")
        if SYSTEM_ALERTS_STREAM not in streams:
            raise ValueError(f"{SYSTEM_ALERTS_STREAM} stream is required for system alerts")
        return streams

    @model_validator(mode="after")
    def validate_timeouts(self) -> Self:
        if self.socket_timeout_ms <= self.consumer_block_ms:
            raise ValueError("Redis socket_timeout_ms must be greater than consumer_block_ms")
        if self.socket_timeout_ms < self.health_timeout_ms:
            raise ValueError("Redis socket_timeout_ms must be greater than or equal to health_timeout_ms")
        return self


class ClickHouseSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str = "localhost"
    port: int = Field(default=8123, ge=1, le=65535)
    username: str = "default"
    password: str = ""
    database: str = "microtrade"
    connect_timeout_seconds: int = Field(default=2, ge=1)
    send_receive_timeout_seconds: int = Field(default=5, ge=1)


class LoggingSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    level: str = "INFO"
    json_logs: bool = Field(default=True, alias="json")

    @field_validator("level")
    @classmethod
    def normalize_level(cls, level: str) -> str:
        return level.upper()


class RetrySettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_attempts: int = Field(default=3, ge=1, le=10)
    base_delay_seconds: float = Field(default=0.1, ge=0)
    max_delay_seconds: float = Field(default=1.0, ge=0)
    backoff_multiplier: float = Field(default=2.0, ge=1)


class MarketDataSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    source_exchange: str = "binance_spot"
    symbols: tuple[str, ...] = ("BTCUSDT",)
    websocket_base_url: str = "wss://stream.binance.com:9443/stream"
    rest_base_url: str = "https://api.binance.com"
    depth_stream_interval_ms: int = Field(default=100, ge=100, le=1000)
    orderbook_levels: int = Field(default=10, ge=10, le=20)
    orderbook_snapshot_interval_ms: int = Field(default=250, ge=100, le=1000)
    snapshot_limit: int = Field(default=5000, ge=100, le=5000)
    queue_max_size_per_symbol: int = Field(default=1000, ge=100, le=100_000)
    snapshot_queue_max_size_per_symbol: int = Field(default=8, ge=1, le=128)
    trade_queue_put_timeout_ms: int = Field(default=5, ge=1, le=1_000)
    clickhouse_batch_size: int = Field(default=500, ge=1, le=10_000)
    clickhouse_flush_interval_ms: int = Field(default=500, ge=50, le=10_000)
    clickhouse_flush_timeout_seconds: float = Field(default=5.0, ge=0.1, le=60)
    reconnect_base_delay_seconds: float = Field(default=1.0, ge=0.1, le=60)
    reconnect_max_delay_seconds: float = Field(default=30.0, ge=1, le=300)
    reconnect_jitter_seconds: float = Field(default=0.25, ge=0, le=5)
    websocket_message_timeout_seconds: int = Field(default=60, ge=5, le=300)
    rest_snapshot_min_interval_ms: int = Field(default=250, ge=50, le=10_000)
    rest_request_weight_limit_per_minute: int = Field(default=6_000, ge=100, le=20_000)
    rest_retry_after_max_seconds: int = Field(default=300, ge=1, le=3_600)
    resync_cooldown_ms: int = Field(default=1_000, ge=100, le=60_000)
    health_stale_after_ms: int = Field(default=30_000, ge=1_000, le=300_000)
    shutdown_drain_timeout_seconds: float = Field(default=5.0, ge=0.5, le=60)

    @field_validator("symbols", mode="before")
    @classmethod
    def parse_symbols(cls, symbols: object) -> object:
        if isinstance(symbols, str):
            return tuple(symbol.strip() for symbol in symbols.split(",") if symbol.strip())
        return symbols

    @field_validator("symbols")
    @classmethod
    def validate_symbols(cls, symbols: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(symbol.strip().upper() for symbol in symbols if symbol.strip())
        if not normalized:
            raise ValueError("at least one market data symbol must be configured")
        if len(normalized) > 5:
            raise ValueError("market data supports at most 5 symbols in Phase 2")
        if len(set(normalized)) != len(normalized):
            raise ValueError("market data symbols must be unique")
        return normalized

    @field_validator("source_exchange")
    @classmethod
    def validate_source_exchange(cls, source_exchange: str) -> str:
        normalized = source_exchange.strip()
        if normalized != "binance_spot":
            raise ValueError("Phase 2 supports only binance_spot")
        return normalized

    @model_validator(mode="after")
    def validate_orderbook_settings(self) -> Self:
        if self.orderbook_levels > self.snapshot_limit:
            raise ValueError("orderbook_levels cannot exceed snapshot_limit")
        if self.reconnect_base_delay_seconds > self.reconnect_max_delay_seconds:
            raise ValueError("reconnect_base_delay_seconds cannot exceed reconnect_max_delay_seconds")
        return self


class AppSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    app_name: str = "microtrade-crypto-ia"
    environment: str = "local"
    api: ApiSettings = Field(default_factory=ApiSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    clickhouse: ClickHouseSettings = Field(default_factory=ClickHouseSettings)
    market_data: MarketDataSettings = Field(default_factory=MarketDataSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    retry: RetrySettings = Field(default_factory=RetrySettings)


def load_settings(
    config_path: str | Path | None = None,
    env_file: str | Path | None = None,
    environ: dict[str, str] | None = None,
) -> AppSettings:
    """Load YAML settings, then override them with .env and process environment values."""

    environment = dict(os.environ if environ is None else environ)
    resolved_config_path = Path(
        config_path or environment.get("MICROTRADE_CONFIG_FILE", "config/default.yaml")
    )
    resolved_env_file = Path(env_file or ".env")

    data = _load_yaml_file(resolved_config_path)
    env_values = _load_env_file(resolved_env_file)
    env_values.update(environment)
    _apply_environment_overrides(data, env_values)
    return AppSettings.model_validate(data)


def _load_yaml_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"settings file must contain a mapping: {path}")
    return loaded


def _load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = _strip_optional_quotes(value.strip())
    return values


def _apply_environment_overrides(data: dict[str, Any], environ: dict[str, str]) -> None:
    prefix = "MICROTRADE_"
    allowed_top_level_keys = {
        "app_name",
        "environment",
        "api",
        "redis",
        "clickhouse",
        "market_data",
        "logging",
        "retry",
    }
    for key, raw_value in environ.items():
        if not key.startswith(prefix):
            continue
        setting_path = key.removeprefix(prefix).lower().split("__")
        if setting_path == ["config_file"]:
            continue
        if setting_path[0] not in allowed_top_level_keys:
            continue
        _set_nested_value(data, setting_path, _coerce_env_value(raw_value))


def _set_nested_value(data: dict[str, Any], path: list[str], value: Any) -> None:
    current = data
    for part in path[:-1]:
        nested = current.setdefault(part, {})
        if not isinstance(nested, dict):
            raise ValueError(f"cannot set nested config value below non-mapping key: {part}")
        current = nested
    current[path[-1]] = value


def _coerce_env_value(value: str) -> Any:
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if value == "":
        return ""
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def _strip_optional_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value
