"""Retry logic: exponential backoff, circuit breaker, and dead-letter queue."""

from __future__ import annotations

import asyncio
import functools
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Type

from dataflow.context import PipelineContext
from dataflow.errors import CircuitOpenError, RetryExhaustedError

logger = logging.getLogger("dataflow.retry")


# ---------------------------------------------------------------------------
# RetryPolicy
# ---------------------------------------------------------------------------

@dataclass
class RetryPolicy:
    """Configurable retry policy with exponential backoff and jitter.

    Parameters:
        max_attempts: Maximum number of attempts (including the first).
        base_delay: Initial delay in seconds.
        max_delay: Cap on the backoff delay.
        exponential_base: Multiplier for exponential growth.
        jitter: If True, add random jitter to the delay.
        retryable_exceptions: Tuple of exception types that trigger a retry.
            Defaults to ``(Exception,)`` which retries everything.
    """

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    retryable_exceptions: tuple[Type[Exception], ...] = (Exception,)

    def compute_delay(self, attempt: int) -> float:
        """Return the delay before the *attempt*-th retry (0-indexed)."""
        delay = self.base_delay * (self.exponential_base ** attempt)
        delay = min(delay, self.max_delay)
        if self.jitter:
            delay = delay * random.uniform(0.5, 1.0)
        return delay


async def retry_async(
    fn: Callable[..., Any],
    *args: Any,
    policy: Optional[RetryPolicy] = None,
    **kwargs: Any,
) -> Any:
    """Execute an async callable with retry logic.

    Returns the first successful result or raises ``RetryExhaustedError``.
    """
    policy = policy or RetryPolicy()
    last_error: Optional[Exception] = None

    for attempt in range(policy.max_attempts):
        try:
            result = fn(*args, **kwargs)
            if asyncio.iscoroutine(result):
                result = await result
            return result
        except policy.retryable_exceptions as exc:
            last_error = exc
            if attempt < policy.max_attempts - 1:
                delay = policy.compute_delay(attempt)
                logger.warning(
                    "Attempt %d/%d failed (%s), retrying in %.2fs",
                    attempt + 1,
                    policy.max_attempts,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)

    raise RetryExhaustedError(
        f"All {policy.max_attempts} attempts failed",
        attempts=policy.max_attempts,
        last_error=last_error,
    )


def with_retry(policy: Optional[RetryPolicy] = None) -> Callable:
    """Decorator that adds retry logic to an async function."""
    policy_ = policy or RetryPolicy()

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await retry_async(fn, *args, policy=policy_, **kwargs)

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# RetryStage
# ---------------------------------------------------------------------------

@dataclass
class RetryStage:
    """Wrap another stage with retry logic.

    If the inner stage raises a retryable exception the record is retried
    according to the given policy.
    """

    inner: Any  # Stage protocol
    policy: RetryPolicy = field(default_factory=RetryPolicy)
    name: str = ""

    def __post_init__(self) -> None:
        if not self.name:
            self.name = f"retry({getattr(self.inner, 'name', 'unknown')})"

    async def process(self, record: Any, ctx: PipelineContext) -> list[Any]:
        return await retry_async(
            self.inner.process, record, ctx, policy=self.policy
        )


# ---------------------------------------------------------------------------
# CircuitBreaker
# ---------------------------------------------------------------------------

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    """Circuit breaker that wraps a stage.

    The breaker trips (opens) after *failure_threshold* consecutive
    failures and stays open for *recovery_timeout* seconds before
    allowing a single probe request (half-open state).

    Parameters:
        inner: The stage to protect.
        failure_threshold: Failures before tripping.
        recovery_timeout: Seconds to wait in the open state.
        success_threshold: Successes in half-open to close.
    """

    inner: Any  # Stage protocol
    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    success_threshold: int = 2
    name: str = ""

    _state: CircuitState = field(default=CircuitState.CLOSED, repr=False)
    _failure_count: int = field(default=0, repr=False)
    _success_count: int = field(default=0, repr=False)
    _last_failure_time: float = field(default=0.0, repr=False)

    def __post_init__(self) -> None:
        if not self.name:
            self.name = f"circuit({getattr(self.inner, 'name', 'unknown')})"

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN:
            if (time.monotonic() - self._last_failure_time) >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                self._success_count = 0
                logger.info("Circuit breaker %s transitioning to HALF_OPEN", self.name)
        return self._state

    async def process(self, record: Any, ctx: PipelineContext) -> list[Any]:
        current = self.state

        if current == CircuitState.OPEN:
            raise CircuitOpenError(
                f"Circuit breaker '{self.name}' is open — dropping record"
            )

        try:
            result = await self.inner.process(record, ctx)
            self._on_success()
            return result
        except Exception as exc:
            self._on_failure()
            raise

    def _on_success(self) -> None:
        self._failure_count = 0
        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.success_threshold:
                self._state = CircuitState.CLOSED
                logger.info("Circuit breaker %s CLOSED", self.name)

    def _on_failure(self) -> None:
        self._failure_count += 1
        self._last_failure_time = time.monotonic()
        if self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN
            logger.warning(
                "Circuit breaker %s OPEN after %d failures",
                self.name,
                self._failure_count,
            )

    def reset(self) -> None:
        """Manually reset the circuit breaker to closed."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0


# ---------------------------------------------------------------------------
# DeadLetterQueue
# ---------------------------------------------------------------------------

@dataclass
class DeadLetterQueue:
    """Collects records that failed processing.

    Can optionally persist failed records to a file.
    """

    max_size: int = 10000
    persist_path: Optional[str] = None
    _items: list[dict[str, Any]] = field(default_factory=list, repr=False)

    def add(self, record: Any, error: Exception, stage_name: str = "") -> None:
        """Add a failed record to the queue."""
        entry = {
            "record": record,
            "error": str(error),
            "error_type": type(error).__name__,
            "stage": stage_name,
            "timestamp": time.time(),
        }
        self._items.append(entry)
        if len(self._items) > self.max_size:
            self._items.pop(0)  # drop oldest

    @property
    def items(self) -> list[dict[str, Any]]:
        return list(self._items)

    @property
    def size(self) -> int:
        return len(self._items)

    def clear(self) -> None:
        self._items.clear()

    async def persist(self) -> None:
        """Write the DLQ contents to a JSONL file."""
        if not self.persist_path or not self._items:
            return

        import json
        from pathlib import Path

        path = Path(self.persist_path).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)

        loop = asyncio.get_running_loop()
        lines = "\n".join(
            json.dumps(item, default=str) for item in self._items
        ) + "\n"
        await loop.run_in_executor(
            None, lambda: path.write_text(lines, encoding="utf-8")
        )
        logger.info("Persisted %d dead-letter records to %s", len(self._items), path)
