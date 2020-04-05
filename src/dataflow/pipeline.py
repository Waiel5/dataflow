"""Core pipeline engine with async-first execution."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Callable, Optional, Protocol, runtime_checkable

from dataflow.context import PipelineContext
from dataflow.errors import ConfigurationError, PipelineError, StageError


# ---------------------------------------------------------------------------
# Protocols
# ---------------------------------------------------------------------------

@runtime_checkable
class Source(Protocol):
    """A source produces records for the pipeline."""

    async def read(self, ctx: PipelineContext) -> AsyncIterator[Any]:
        ...  # pragma: no cover


@runtime_checkable
class Stage(Protocol):
    """A stage transforms a single record, returning zero or more records."""

    name: str

    async def process(self, record: Any, ctx: PipelineContext) -> list[Any]:
        ...  # pragma: no cover


@runtime_checkable
class Sink(Protocol):
    """A sink consumes records at the end of the pipeline."""

    async def write(self, record: Any, ctx: PipelineContext) -> None:
        ...  # pragma: no cover

    async def flush(self, ctx: PipelineContext) -> None:
        ...  # pragma: no cover


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

@dataclass
class Pipeline:
    """Composable async data pipeline.

    Usage::

        p = Pipeline("my-etl")
        p.add_source(CSVSource("data.csv"))
        p.add_stage(FilterStage(lambda r: r["active"]))
        p.add_stage(MapStage(transform))
        p.add_sink(JSONSink("out.json"))
        await p.run()

    The pipeline streams records one at a time through the stage chain,
    applying optional backpressure via an internal asyncio.Queue.
    """

    name: str
    source: Optional[Source] = field(default=None, repr=False)
    stages: list[Stage] = field(default_factory=list)
    sinks: list[Sink] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)
    max_queue_size: int = 1000
    on_error: str = "raise"  # "raise" | "skip" | "dead_letter"
    _dead_letter: list[dict[str, Any]] = field(default_factory=list, repr=False)

    # -- builder API --------------------------------------------------------

    def add_source(self, source: Source) -> "Pipeline":
        """Set the pipeline source (only one allowed)."""
        self.source = source
        return self

    def add_stage(self, stage: Stage) -> "Pipeline":
        """Append a processing stage."""
        self.stages.append(stage)
        return self

    def add_sink(self, sink: Sink) -> "Pipeline":
        """Append an output sink."""
        self.sinks.append(sink)
        return self

    def set_error_strategy(self, strategy: str) -> "Pipeline":
        """Configure error handling: 'raise', 'skip', or 'dead_letter'."""
        if strategy not in ("raise", "skip", "dead_letter"):
            raise ConfigurationError(f"Unknown error strategy: {strategy}")
        self.on_error = strategy
        return self

    @property
    def dead_letter_queue(self) -> list[dict[str, Any]]:
        """Records that failed processing when on_error='dead_letter'."""
        return list(self._dead_letter)

    # -- execution ----------------------------------------------------------

    async def run(self) -> PipelineContext:
        """Execute the full pipeline asynchronously.

        Returns the pipeline context which contains run metrics.
        """
        self._validate()
        ctx = PipelineContext(pipeline_name=self.name, config=self.config)
        ctx.start()

        queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=self.max_queue_size)
        sentinel = object()

        async def _produce() -> None:
            assert self.source is not None
            async for record in self.source.read(ctx):
                await queue.put(record)
            await queue.put(sentinel)

        async def _consume() -> None:
            while True:
                record = await queue.get()
                if record is sentinel:
                    break
                await self._process_record(record, ctx)
                queue.task_done()

        producer = asyncio.create_task(_produce())
        consumer = asyncio.create_task(_consume())

        await producer
        await consumer

        # flush all sinks
        for sink in self.sinks:
            await sink.flush(ctx)

        ctx.finish()
        return ctx

    def run_sync(self) -> PipelineContext:
        """Convenience wrapper to run the pipeline synchronously."""
        return asyncio.run(self.run())

    # -- internal -----------------------------------------------------------

    def _validate(self) -> None:
        if self.source is None:
            raise ConfigurationError("Pipeline requires a source")
        if not self.sinks:
            raise ConfigurationError("Pipeline requires at least one sink")

    async def _process_record(self, record: Any, ctx: PipelineContext) -> None:
        """Push a single record through all stages then into every sink."""
        records = [record]

        for stage in self.stages:
            next_records: list[Any] = []
            for rec in records:
                try:
                    result = await stage.process(rec, ctx)
                    next_records.extend(result)
                except Exception as exc:
                    if self.on_error == "raise":
                        raise StageError(
                            f"Stage '{stage.name}' failed: {exc}",
                            stage_name=stage.name,
                            record=rec,
                        ) from exc
                    elif self.on_error == "dead_letter":
                        self._dead_letter.append(
                            {
                                "stage": stage.name,
                                "record": rec,
                                "error": str(exc),
                            }
                        )
                    # on_error == "skip" -> silently drop the record
            records = next_records
            if not records:
                return

        # write surviving records to all sinks
        for rec in records:
            ctx.increment_count()
            for sink in self.sinks:
                await sink.write(rec, ctx)
