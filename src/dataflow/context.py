"""Pipeline context: configuration, state, and logging."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class PipelineContext:
    """Carries shared state and configuration through a pipeline run.

    The context is created when a pipeline starts and is passed to every
    stage.  Stages can read configuration values and store intermediate
    state that later stages or the pipeline runner can inspect.
    """

    pipeline_name: str
    config: dict[str, Any] = field(default_factory=dict)
    state: dict[str, Any] = field(default_factory=dict)
    logger: logging.Logger = field(init=False)
    start_time: float = field(default=0.0)
    _record_count: int = field(default=0, repr=False)

    def __post_init__(self) -> None:
        self.logger = logging.getLogger(f"dataflow.{self.pipeline_name}")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def start(self) -> None:
        """Mark the beginning of a pipeline run."""
        self.start_time = time.monotonic()
        self._record_count = 0
        self.logger.info("Pipeline '%s' started", self.pipeline_name)

    def finish(self) -> None:
        """Mark the end of a pipeline run and log summary."""
        elapsed = time.monotonic() - self.start_time
        self.logger.info(
            "Pipeline '%s' finished: %d records in %.2fs",
            self.pipeline_name,
            self._record_count,
            elapsed,
        )

    def increment_count(self, n: int = 1) -> None:
        self._record_count += n

    @property
    def record_count(self) -> int:
        return self._record_count

    @property
    def elapsed(self) -> float:
        """Seconds since the pipeline started."""
        if self.start_time == 0.0:
            return 0.0
        return time.monotonic() - self.start_time

    def get_config(self, key: str, default: Any = None) -> Any:
        """Retrieve a config value with an optional default."""
        return self.config.get(key, default)

    def set_state(self, key: str, value: Any) -> None:
        """Store a value in the shared state dict."""
        self.state[key] = value

    def get_state(self, key: str, default: Any = None) -> Any:
        """Retrieve a value from the shared state dict."""
        return self.state.get(key, default)
