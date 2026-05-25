from __future__ import annotations

import time


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def latency_ms(start_ms: int, end_ms: int | None = None) -> float:
    current_ms = now_ms() if end_ms is None else end_ms
    return float(max(current_ms - start_ms, 0))

