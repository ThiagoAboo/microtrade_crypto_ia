from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay_seconds: float = 0.1
    max_delay_seconds: float = 1.0
    backoff_multiplier: float = 2.0


async def retry_async(
    operation: Callable[[], Awaitable[T]],
    policy: RetryPolicy,
    *,
    operation_name: str,
    logger: logging.Logger | None = None,
    retry_exceptions: tuple[type[Exception], ...] = (Exception,),
) -> T:
    attempt = 1
    delay = policy.base_delay_seconds

    while True:
        try:
            return await operation()
        except retry_exceptions:
            if attempt >= policy.max_attempts:
                raise
            if logger is not None:
                logger.warning(
                    "retrying failed operation",
                    extra={
                        "operation": operation_name,
                        "attempt": attempt,
                        "next_delay_seconds": delay,
                        "category": "SYSTEM",
                    },
                    exc_info=True,
                )
            await asyncio.sleep(delay)
            delay = min(delay * policy.backoff_multiplier, policy.max_delay_seconds)
            attempt += 1

