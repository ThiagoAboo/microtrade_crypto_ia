from __future__ import annotations

import argparse
import json
import subprocess
import time
from dataclasses import asdict, dataclass
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen


@dataclass(slots=True)
class Sample:
    elapsed_seconds: float
    healthy: bool
    memory_mb: float | None
    cpu_percent: float | None
    queue_lag: int
    drops: int
    reconnects: int
    resyncs: int
    throughput_per_second: float


def main() -> int:
    args = _parse_args()
    service_ids = _docker_compose_service_ids(args.compose_services)
    samples: list[Sample] = []
    started_at = time.perf_counter()
    previous_events: int | None = None
    previous_elapsed: float | None = None

    while True:
        elapsed = time.perf_counter() - started_at
        if elapsed > args.duration_seconds:
            break

        health = _fetch_json(f"{args.api_url.rstrip('/')}/health/market-data")
        stats = _docker_stats(service_ids) if service_ids else {}
        current_events = _event_count(health)
        throughput = 0.0
        if previous_events is not None and previous_elapsed is not None:
            delta_events = max(current_events - previous_events, 0)
            delta_seconds = max(elapsed - previous_elapsed, 0.001)
            throughput = delta_events / delta_seconds
        previous_events = current_events
        previous_elapsed = elapsed

        sample = Sample(
            elapsed_seconds=round(elapsed, 3),
            healthy=health.get("status") == "running",
            memory_mb=_total_memory_mb(stats),
            cpu_percent=_total_cpu_percent(stats),
            queue_lag=_queue_lag(health),
            drops=_drops(health),
            reconnects=int(health.get("reconnect_count") or 0),
            resyncs=_resyncs(health),
            throughput_per_second=round(throughput, 3),
        )
        samples.append(sample)
        print(json.dumps(asdict(sample), separators=(",", ":")), flush=True)
        time.sleep(args.sample_interval_seconds)

    print(json.dumps(_summary(samples), indent=2), flush=True)
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lightweight Market Data soak sampler.")
    parser.add_argument("--api-url", default="http://127.0.0.1:8000")
    parser.add_argument("--duration-seconds", type=int, default=300)
    parser.add_argument("--sample-interval-seconds", type=int, default=5)
    parser.add_argument(
        "--compose-services",
        default="api,redis,clickhouse",
        help="Comma separated Docker Compose services to include in CPU/RAM stats.",
    )
    return parser.parse_args()


def _fetch_json(url: str) -> dict[str, Any]:
    try:
        with urlopen(url, timeout=2) as response:
            return json.loads(response.read().decode("utf-8"))
    except (TimeoutError, URLError, json.JSONDecodeError) as exc:
        return {"status": "unreachable", "error": str(exc), "symbols": {}}


def _docker_compose_service_ids(raw_services: str) -> list[str]:
    service_ids: list[str] = []
    for service in [item.strip() for item in raw_services.split(",") if item.strip()]:
        try:
            result = subprocess.run(
                ["docker", "compose", "ps", "-q", service],
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        service_ids.extend(line.strip() for line in result.stdout.splitlines() if line.strip())
    return service_ids


def _docker_stats(container_ids: list[str]) -> dict[str, dict[str, str]]:
    try:
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}", *container_ids],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return {}

    stats: dict[str, dict[str, str]] = {}
    for line in result.stdout.splitlines():
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        stats[str(payload.get("Name") or payload.get("ID"))] = payload
    return stats


def _total_memory_mb(stats: dict[str, dict[str, str]]) -> float | None:
    if not stats:
        return None
    return round(sum(_parse_memory_mb(item.get("MemUsage", "")) for item in stats.values()), 3)


def _parse_memory_mb(raw: str) -> float:
    used = raw.split("/", 1)[0].strip()
    if used.endswith("GiB"):
        return float(used.removesuffix("GiB")) * 1024
    if used.endswith("MiB"):
        return float(used.removesuffix("MiB"))
    if used.endswith("KiB"):
        return float(used.removesuffix("KiB")) / 1024
    if used.endswith("B"):
        return float(used.removesuffix("B")) / (1024 * 1024)
    return 0.0


def _total_cpu_percent(stats: dict[str, dict[str, str]]) -> float | None:
    if not stats:
        return None
    total = 0.0
    for item in stats.values():
        raw = str(item.get("CPUPerc", "0")).strip().removesuffix("%")
        try:
            total += float(raw)
        except ValueError:
            continue
    return round(total, 3)


def _symbols(health: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw_symbols = health.get("symbols")
    return raw_symbols if isinstance(raw_symbols, dict) else {}


def _queue_lag(health: dict[str, Any]) -> int:
    return sum(int(item.get("queue_lag") or 0) for item in _symbols(health).values())


def _drops(health: dict[str, Any]) -> int:
    total = 0
    for item in _symbols(health).values():
        total += int(item.get("dropped_trades") or 0)
        total += int(item.get("dropped_requeued_trades") or 0)
        total += int(item.get("dropped_snapshots") or 0)
    return total


def _resyncs(health: dict[str, Any]) -> int:
    return sum(int(item.get("resync_count") or 0) for item in _symbols(health).values())


def _event_count(health: dict[str, Any]) -> int:
    total = 0
    for item in _symbols(health).values():
        total += int(item.get("trades_received") or 0)
        total += int(item.get("depth_updates_received") or 0)
        total += int(item.get("snapshots_published") or 0)
    return total


def _summary(samples: list[Sample]) -> dict[str, Any]:
    if not samples:
        return {"samples": 0}
    memory_values = [sample.memory_mb for sample in samples if sample.memory_mb is not None]
    cpu_values = [sample.cpu_percent for sample in samples if sample.cpu_percent is not None]
    return {
        "samples": len(samples),
        "duration_seconds": samples[-1].elapsed_seconds,
        "max_queue_lag": max(sample.queue_lag for sample in samples),
        "max_drops": max(sample.drops for sample in samples),
        "max_reconnects": max(sample.reconnects for sample in samples),
        "max_resyncs": max(sample.resyncs for sample in samples),
        "max_throughput_per_second": max(sample.throughput_per_second for sample in samples),
        "memory_mb_start": memory_values[0] if memory_values else None,
        "memory_mb_end": memory_values[-1] if memory_values else None,
        "memory_mb_growth": (
            round(memory_values[-1] - memory_values[0], 3) if len(memory_values) >= 2 else None
        ),
        "cpu_percent_max": max(cpu_values) if cpu_values else None,
    }


if __name__ == "__main__":
    raise SystemExit(main())
