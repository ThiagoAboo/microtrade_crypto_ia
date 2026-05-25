from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict


HealthState = Literal["ok", "unhealthy"]


class DependencyHealth(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    status: HealthState
    latency_ms: float
    error: str | None = None


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: HealthState
    service: str
    environment: str
    dependencies: list[DependencyHealth]
    latency_ms: float

