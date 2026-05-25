import asyncio

from core.retry import RetryPolicy, retry_async


def test_retry_async_retries_until_success() -> None:
    calls = 0

    async def flaky_operation() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ValueError("temporary failure")
        return "ok"

    result = asyncio.run(
        retry_async(
            flaky_operation,
            RetryPolicy(max_attempts=3, base_delay_seconds=0, max_delay_seconds=0),
            operation_name="unit.flaky",
            retry_exceptions=(ValueError,),
        )
    )

    assert result == "ok"
    assert calls == 3
