"""Tests for the core pipeline engine."""

from __future__ import annotations

import asyncio
import pytest
from dataflow import (
    Pipeline,
    IterableSource,
    MapStage,
    FilterStage,
    CallbackSink,
    JSONSink,
    ConfigurationError,
    StageError,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class CollectorSink:
    """Collects records for assertions."""

    def __init__(self):
        self.records = []

    async def write(self, record, ctx):
        self.records.append(record)

    async def flush(self, ctx):
        pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_simple_pipeline():
    """Records flow from source through stages to sink."""
    data = [1, 2, 3, 4, 5]
    sink = CollectorSink()

    p = Pipeline("test-simple")
    p.add_source(IterableSource(data))
    p.add_stage(MapStage(lambda x: x * 2))
    p.add_sink(sink)

    ctx = await p.run()
    assert sink.records == [2, 4, 6, 8, 10]
    assert ctx.record_count == 5


@pytest.mark.asyncio
async def test_filter_stage():
    """FilterStage drops records that do not match the predicate."""
    data = [1, 2, 3, 4, 5, 6]
    sink = CollectorSink()

    p = Pipeline("test-filter")
    p.add_source(IterableSource(data))
    p.add_stage(FilterStage(lambda x: x % 2 == 0))
    p.add_sink(sink)

    await p.run()
    assert sink.records == [2, 4, 6]


@pytest.mark.asyncio
async def test_chained_stages():
    """Multiple stages compose correctly."""
    data = [{"name": "alice", "age": 30}, {"name": "bob", "age": 17}, {"name": "carol", "age": 25}]
    sink = CollectorSink()

    p = Pipeline("test-chain")
    p.add_source(IterableSource(data))
    p.add_stage(FilterStage(lambda r: r["age"] >= 18))
    p.add_stage(MapStage(lambda r: {**r, "name": r["name"].upper()}))
    p.add_sink(sink)

    await p.run()
    assert len(sink.records) == 2
    assert sink.records[0]["name"] == "ALICE"
    assert sink.records[1]["name"] == "CAROL"


@pytest.mark.asyncio
async def test_pipeline_requires_source():
    """Pipeline.run raises ConfigurationError without a source."""
    p = Pipeline("no-source")
    p.add_sink(CollectorSink())
    with pytest.raises(ConfigurationError, match="requires a source"):
        await p.run()


@pytest.mark.asyncio
async def test_pipeline_requires_sink():
    """Pipeline.run raises ConfigurationError without a sink."""
    p = Pipeline("no-sink")
    p.add_source(IterableSource([1]))
    with pytest.raises(ConfigurationError, match="requires at least one sink"):
        await p.run()


@pytest.mark.asyncio
async def test_error_strategy_skip():
    """on_error='skip' silently drops failing records."""

    def blow_up(x):
        if x == 3:
            raise ValueError("boom")
        return x

    sink = CollectorSink()
    p = Pipeline("test-skip")
    p.add_source(IterableSource([1, 2, 3, 4]))
    p.add_stage(MapStage(blow_up))
    p.add_sink(sink)
    p.set_error_strategy("skip")

    await p.run()
    assert sink.records == [1, 2, 4]


@pytest.mark.asyncio
async def test_error_strategy_dead_letter():
    """on_error='dead_letter' routes failures to the DLQ."""

    def blow_up(x):
        if x == 2:
            raise ValueError("bad record")
        return x

    sink = CollectorSink()
    p = Pipeline("test-dlq")
    p.add_source(IterableSource([1, 2, 3]))
    p.add_stage(MapStage(blow_up))
    p.add_sink(sink)
    p.set_error_strategy("dead_letter")

    await p.run()
    assert sink.records == [1, 3]
    assert len(p.dead_letter_queue) == 1
    assert p.dead_letter_queue[0]["stage"] == "map"


@pytest.mark.asyncio
async def test_error_strategy_raise():
    """on_error='raise' propagates exceptions."""

    sink = CollectorSink()
    p = Pipeline("test-raise")
    p.add_source(IterableSource([1, 2]))
    p.add_stage(MapStage(lambda x: 1 / 0))
    p.add_sink(sink)

    with pytest.raises(StageError):
        await p.run()


@pytest.mark.asyncio
async def test_multiple_sinks():
    """Records are written to every sink."""
    sink_a = CollectorSink()
    sink_b = CollectorSink()

    p = Pipeline("test-multi-sink")
    p.add_source(IterableSource([10, 20]))
    p.add_sink(sink_a)
    p.add_sink(sink_b)

    await p.run()
    assert sink_a.records == [10, 20]
    assert sink_b.records == [10, 20]


@pytest.mark.asyncio
async def test_builder_chaining():
    """Builder methods return the pipeline for fluent chaining."""
    sink = CollectorSink()

    p = (
        Pipeline("test-fluent")
        .add_source(IterableSource([1]))
        .add_stage(MapStage(lambda x: x))
        .add_sink(sink)
    )

    ctx = await p.run()
    assert ctx.record_count == 1


@pytest.mark.asyncio
async def test_async_stage():
    """Stages can use async functions."""

    async def async_double(x):
        await asyncio.sleep(0)
        return x * 2

    sink = CollectorSink()
    p = Pipeline("test-async")
    p.add_source(IterableSource([5, 10]))
    p.add_stage(MapStage(async_double))
    p.add_sink(sink)

    await p.run()
    assert sink.records == [10, 20]


def test_run_sync():
    """run_sync provides a synchronous entry point."""
    sink = CollectorSink()
    p = Pipeline("test-sync")
    p.add_source(IterableSource([1, 2, 3]))
    p.add_stage(MapStage(lambda x: x + 1))
    p.add_sink(sink)

    ctx = p.run_sync()
    assert sink.records == [2, 3, 4]
    assert ctx.record_count == 3
