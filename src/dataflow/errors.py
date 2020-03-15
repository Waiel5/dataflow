"""Custom exception hierarchy for dataflow pipelines."""

from __future__ import annotations

from typing import Any, Optional


class DataflowError(Exception):
    """Base exception for all dataflow errors."""

    def __init__(self, message: str, *, details: Optional[dict[str, Any]] = None) -> None:
        self.details = details or {}
        super().__init__(message)


class PipelineError(DataflowError):
    """Raised when a pipeline-level error occurs."""

    pass


class StageError(DataflowError):
    """Raised when a stage fails to process a record."""

    def __init__(
        self,
        message: str,
        *,
        stage_name: str = "",
        record: Any = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        self.stage_name = stage_name
        self.record = record
        super().__init__(message, details=details)


class SourceError(DataflowError):
    """Raised when a source fails to produce records."""

    pass


class SinkError(DataflowError):
    """Raised when a sink fails to consume records."""

    pass


class RetryExhaustedError(DataflowError):
    """Raised when all retry attempts have been exhausted."""

    def __init__(
        self,
        message: str,
        *,
        attempts: int = 0,
        last_error: Optional[Exception] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(message, details=details)


class CircuitOpenError(DataflowError):
    """Raised when the circuit breaker is in the open state."""

    pass


class ValidationError(StageError):
    """Raised when a record fails schema validation."""

    def __init__(
        self,
        message: str,
        *,
        field: str = "",
        expected: Any = None,
        got: Any = None,
        stage_name: str = "",
        record: Any = None,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        self.field = field
        self.expected = expected
        self.got = got
        super().__init__(message, stage_name=stage_name, record=record, details=details)


class BackpressureError(DataflowError):
    """Raised when backpressure limits are exceeded."""

    pass


class ConfigurationError(DataflowError):
    """Raised when pipeline configuration is invalid."""

    pass
