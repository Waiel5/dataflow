"""Metrics collection and Prometheus-compatible export."""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Counter
# ---------------------------------------------------------------------------

@dataclass
class Counter:
    """A monotonically increasing counter."""

    name: str
    labels: dict[str, str] = field(default_factory=dict)
    _value: float = field(default=0.0, repr=False)

    def inc(self, amount: float = 1.0) -> None:
        self._value += amount

    @property
    def value(self) -> float:
        return self._value

    def reset(self) -> None:
        self._value = 0.0


# ---------------------------------------------------------------------------
# Gauge
# ---------------------------------------------------------------------------

@dataclass
class Gauge:
    """A value that can go up and down."""

    name: str
    labels: dict[str, str] = field(default_factory=dict)
    _value: float = field(default=0.0, repr=False)

    def set(self, value: float) -> None:
        self._value = value

    def inc(self, amount: float = 1.0) -> None:
        self._value += amount

    def dec(self, amount: float = 1.0) -> None:
        self._value -= amount

    @property
    def value(self) -> float:
        return self._value


# ---------------------------------------------------------------------------
# Histogram
# ---------------------------------------------------------------------------

@dataclass
class Histogram:
    """Track value distributions with configurable buckets.

    Default buckets are modeled after Prometheus defaults.
    """

    name: str
    labels: dict[str, str] = field(default_factory=dict)
    buckets: tuple[float, ...] = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
    _counts: dict[float, int] = field(default_factory=dict, repr=False)
    _sum: float = field(default=0.0, repr=False)
    _count: int = field(default=0, repr=False)

    def __post_init__(self) -> None:
        self._counts = {b: 0 for b in self.buckets}
        self._counts[float("inf")] = 0

    def observe(self, value: float) -> None:
        self._sum += value
        self._count += 1
        for b in self.buckets:
            if value <= b:
                self._counts[b] += 1
        self._counts[float("inf")] += 1

    @property
    def count(self) -> int:
        return self._count

    @property
    def sum(self) -> float:
        return self._sum

    @property
    def mean(self) -> float:
        return self._sum / self._count if self._count else 0.0


# ---------------------------------------------------------------------------
# StageMetrics
# ---------------------------------------------------------------------------

@dataclass
class StageMetrics:
    """Metrics for a single pipeline stage."""

    stage_name: str
    records_in: Counter = field(init=False)
    records_out: Counter = field(init=False)
    errors: Counter = field(init=False)
    latency: Histogram = field(init=False)

    def __post_init__(self) -> None:
        labels = {"stage": self.stage_name}
        self.records_in = Counter("stage_records_in", labels=labels)
        self.records_out = Counter("stage_records_out", labels=labels)
        self.errors = Counter("stage_errors", labels=labels)
        self.latency = Histogram("stage_latency_seconds", labels=labels)


# ---------------------------------------------------------------------------
# MetricsCollector
# ---------------------------------------------------------------------------

@dataclass
class MetricsCollector:
    """Central metrics collector for an entire pipeline run.

    Tracks per-stage metrics plus pipeline-level counters.
    """

    pipeline_name: str
    total_records: Counter = field(init=False)
    total_errors: Counter = field(init=False)
    pipeline_duration: Gauge = field(init=False)
    throughput: Gauge = field(init=False)
    _stage_metrics: dict[str, StageMetrics] = field(default_factory=dict, repr=False)
    _start_time: float = field(default=0.0, repr=False)

    def __post_init__(self) -> None:
        labels = {"pipeline": self.pipeline_name}
        self.total_records = Counter("pipeline_total_records", labels=labels)
        self.total_errors = Counter("pipeline_total_errors", labels=labels)
        self.pipeline_duration = Gauge("pipeline_duration_seconds", labels=labels)
        self.throughput = Gauge("pipeline_throughput_rps", labels=labels)

    def start(self) -> None:
        self._start_time = time.monotonic()

    def finish(self) -> None:
        elapsed = time.monotonic() - self._start_time
        self.pipeline_duration.set(elapsed)
        if elapsed > 0:
            self.throughput.set(self.total_records.value / elapsed)

    def stage(self, name: str) -> StageMetrics:
        """Get or create metrics for a named stage."""
        if name not in self._stage_metrics:
            self._stage_metrics[name] = StageMetrics(stage_name=name)
        return self._stage_metrics[name]

    @property
    def stages(self) -> dict[str, StageMetrics]:
        return dict(self._stage_metrics)

    def to_prometheus(self) -> str:
        """Export all metrics in Prometheus text exposition format."""
        lines: list[str] = []

        # Pipeline-level metrics
        lines.append(f"# HELP pipeline_total_records Total records processed")
        lines.append(f"# TYPE pipeline_total_records counter")
        lines.append(
            f'pipeline_total_records{{pipeline="{self.pipeline_name}"}} '
            f"{self.total_records.value}"
        )

        lines.append(f"# HELP pipeline_total_errors Total processing errors")
        lines.append(f"# TYPE pipeline_total_errors counter")
        lines.append(
            f'pipeline_total_errors{{pipeline="{self.pipeline_name}"}} '
            f"{self.total_errors.value}"
        )

        lines.append(f"# HELP pipeline_duration_seconds Pipeline run duration")
        lines.append(f"# TYPE pipeline_duration_seconds gauge")
        lines.append(
            f'pipeline_duration_seconds{{pipeline="{self.pipeline_name}"}} '
            f"{self.pipeline_duration.value:.6f}"
        )

        lines.append(f"# HELP pipeline_throughput_rps Records per second")
        lines.append(f"# TYPE pipeline_throughput_rps gauge")
        lines.append(
            f'pipeline_throughput_rps{{pipeline="{self.pipeline_name}"}} '
            f"{self.throughput.value:.2f}"
        )

        # Per-stage metrics
        for stage_name, sm in self._stage_metrics.items():
            safe_name = stage_name.replace("-", "_").replace(" ", "_")

            lines.append(f"# HELP stage_records_in Records entering stage")
            lines.append(f"# TYPE stage_records_in counter")
            lines.append(
                f'stage_records_in{{pipeline="{self.pipeline_name}",stage="{safe_name}"}} '
                f"{sm.records_in.value}"
            )

            lines.append(f"# HELP stage_records_out Records leaving stage")
            lines.append(f"# TYPE stage_records_out counter")
            lines.append(
                f'stage_records_out{{pipeline="{self.pipeline_name}",stage="{safe_name}"}} '
                f"{sm.records_out.value}"
            )

            lines.append(f"# HELP stage_errors Stage processing errors")
            lines.append(f"# TYPE stage_errors counter")
            lines.append(
                f'stage_errors{{pipeline="{self.pipeline_name}",stage="{safe_name}"}} '
                f"{sm.errors.value}"
            )

            lines.append(f"# HELP stage_latency_seconds Stage processing latency")
            lines.append(f"# TYPE stage_latency_seconds histogram")
            for bucket, count in sm.latency._counts.items():
                le = "+Inf" if bucket == float("inf") else f"{bucket}"
                lines.append(
                    f'stage_latency_seconds_bucket{{pipeline="{self.pipeline_name}",'
                    f'stage="{safe_name}",le="{le}"}} {count}'
                )
            lines.append(
                f'stage_latency_seconds_sum{{pipeline="{self.pipeline_name}",'
                f'stage="{safe_name}"}} {sm.latency.sum:.6f}'
            )
            lines.append(
                f'stage_latency_seconds_count{{pipeline="{self.pipeline_name}",'
                f'stage="{safe_name}"}} {sm.latency.count}'
            )

        return "\n".join(lines) + "\n"

    def summary(self) -> dict[str, Any]:
        """Return a human-readable summary dict."""
        return {
            "pipeline": self.pipeline_name,
            "total_records": self.total_records.value,
            "total_errors": self.total_errors.value,
            "duration_seconds": round(self.pipeline_duration.value, 3),
            "throughput_rps": round(self.throughput.value, 2),
            "stages": {
                name: {
                    "records_in": sm.records_in.value,
                    "records_out": sm.records_out.value,
                    "errors": sm.errors.value,
                    "avg_latency_ms": round(sm.latency.mean * 1000, 3),
                }
                for name, sm in self._stage_metrics.items()
            },
        }


# ---------------------------------------------------------------------------
# InstrumentedStage (decorator)
# ---------------------------------------------------------------------------

@dataclass
class InstrumentedStage:
    """Wrap a stage with automatic metrics collection."""

    inner: Any  # Stage protocol
    collector: MetricsCollector
    name: str = ""

    def __post_init__(self) -> None:
        if not self.name:
            self.name = getattr(self.inner, "name", "unknown")

    async def process(self, record: Any, ctx: PipelineContext) -> list[Any]:
        sm = self.collector.stage(self.name)
        sm.records_in.inc()
        start = time.monotonic()

        try:
            result = await self.inner.process(record, ctx)
            sm.records_out.inc(len(result))
            return result
        except Exception:
            sm.errors.inc()
            self.collector.total_errors.inc()
            raise
        finally:
            elapsed = time.monotonic() - start
            sm.latency.observe(elapsed)
