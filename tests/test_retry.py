"""Tests for retry logic, circuit breaker, and dead-letter queue."""

from __future__ import annotations

import asyncio
import time

import pytest
from dataflow import (
    RetryPolicy,
    RetryStage,
    CircuitBreaker,
    DeadLetterQueue,
    RetryExhaustedError,
    CircuitOpenError,
    PipelineContext,
    MapStage,
    retry_async,
)


@pytest.fixture
def ctx():
    return PipelineContext(pipeline_name="test-retry")


# ---------------------------------------------------------------------------
# RetryPolicy
# ---------------------------------------------------------------------------

def test_retry_policy_delay():
    policy = RetryPolicy(base_delay=1.0, exponential_base=2.0, jitter=False)
    assert policy.compute_delay(0) == 1.0
    assert policy.compute_delay(1) == 2.0
    assert policy.compute_delay(2) == 4.0


def test_retry_policy_max_delay():
    policy = RetryPolicy(base_delay=1.0, exponential_base=2.0, max_delay=3.0, jitter=False)
    assert policy.compute_delay(5) == 3.0


def test_retry_policy_jitter():
    policy = RetryPolicy(base_delay=10.0, jitter=True)
    delay = policy.compute_delay(0)
    # jitter should put it between 5.0 and 10.0
    assert 5.0 <= delay <= 10.0


# ---------------------------------------------------------------------------
# retry_async
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_retry_async_success_first_try():
    call_count = 0

    async def succeed():
        nonlocal call_count
        call_count += 1
        return "ok"

    result = await retry_async(succeed, policy=RetryPolicy(max_attempts=3))
    assert result == "ok"
    assert call_count == 1


@pytest.mark.asyncio
async def test_retry_async_success_after_retries():
    call_count = 0

    async def fail_twice():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ValueError("not yet")
        return "ok"

    policy = RetryPolicy(max_attempts=5, base_delay=0.01, jitter=False)
    result = await retry_async(fail_twice, policy=policy)
    assert result == "ok"
    assert call_count == 3


@pytest.mark.asyncio
async def test_retry_async_exhausted():
    async def always_fail():
        raise RuntimeError("boom")

    policy = RetryPolicy(max_attempts=2, base_delay=0.01, jitter=False)
    with pytest.raises(RetryExhaustedError) as exc_info:
        await retry_async(always_fail, policy=policy)

    assert exc_info.value.attempts == 2
    assert isinstance(exc_info.value.last_error, RuntimeError)


@pytest.mark.asyncio
async def test_retry_async_selective_exceptions():
    """Only retries on specified exception types."""
    call_count = 0

    async def fail():
        nonlocal call_count
        call_count += 1
        raise TypeError("wrong type")

    policy = RetryPolicy(
        max_attempts=3,
        base_delay=0.01,
        retryable_exceptions=(ValueError,),
        jitter=False,
    )
    # TypeError is not retryable, so it should propagate immediately
    with pytest.raises(TypeError):
        await retry_async(fail, policy=policy)
    assert call_count == 1


# ---------------------------------------------------------------------------
# RetryStage
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_retry_stage(ctx):
    call_count = 0

    class FlakeyStage:
        name = "flakey"

        async def process(self, record, ctx):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("transient")
            return [record * 2]

    policy = RetryPolicy(max_attempts=5, base_delay=0.01, jitter=False)
    stage = RetryStage(inner=FlakeyStage(), policy=policy)

    result = await stage.process(5, ctx)
    assert result == [10]
    assert call_count == 3


# ---------------------------------------------------------------------------
# CircuitBreaker
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_circuit_breaker_stays_closed_on_success(ctx):
    stage = MapStage(lambda x: x)
    cb = CircuitBreaker(inner=stage, failure_threshold=3)

    result = await cb.process(1, ctx)
    assert result == [1]
    assert cb.state.value == "closed"


@pytest.mark.asyncio
async def test_circuit_breaker_opens_after_threshold(ctx):
    class FailStage:
        name = "fail"

        async def process(self, record, ctx):
            raise RuntimeError("fail")

    cb = CircuitBreaker(inner=FailStage(), failure_threshold=3, recovery_timeout=100)

    for _ in range(3):
        with pytest.raises(RuntimeError):
            await cb.process(1, ctx)

    # Circuit should now be open
    with pytest.raises(CircuitOpenError):
        await cb.process(1, ctx)


@pytest.mark.asyncio
async def test_circuit_breaker_half_open(ctx):
    call_count = 0

    class RecoveringStage:
        name = "recovering"

        async def process(self, record, ctx):
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                raise RuntimeError("fail")
            return [record]

    cb = CircuitBreaker(
        inner=RecoveringStage(),
        failure_threshold=3,
        recovery_timeout=0.05,
        success_threshold=1,
    )

    # Trip the breaker
    for _ in range(3):
        with pytest.raises(RuntimeError):
            await cb.process(1, ctx)

    assert cb.state.value == "open"

    # Wait for recovery timeout
    await asyncio.sleep(0.1)

    # Should transition to half_open and succeed
    result = await cb.process(1, ctx)
    assert result == [1]
    assert cb.state.value == "closed"


@pytest.mark.asyncio
async def test_circuit_breaker_reset(ctx):
    class FailStage:
        name = "fail"

        async def process(self, record, ctx):
            raise RuntimeError("fail")

    cb = CircuitBreaker(inner=FailStage(), failure_threshold=2)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await cb.process(1, ctx)

    cb.reset()
    assert cb.state.value == "closed"


# ---------------------------------------------------------------------------
# DeadLetterQueue
# ---------------------------------------------------------------------------

def test_dlq_add_and_retrieve():
    dlq = DeadLetterQueue()
    dlq.add({"id": 1}, ValueError("bad"), stage_name="validate")

    assert dlq.size == 1
    item = dlq.items[0]
    assert item["record"] == {"id": 1}
    assert item["error"] == "bad"
    assert item["error_type"] == "ValueError"
    assert item["stage"] == "validate"


def test_dlq_max_size():
    dlq = DeadLetterQueue(max_size=3)
    for i in range(5):
        dlq.add(i, Exception(f"err-{i}"))

    assert dlq.size == 3
    # oldest should have been dropped
    assert dlq.items[0]["record"] == 2


def test_dlq_clear():
    dlq = DeadLetterQueue()
    dlq.add(1, Exception("e"))
    dlq.add(2, Exception("e"))
    dlq.clear()
    assert dlq.size == 0
