"""Built-in processing stages: map, filter, batch, flatten, window."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional, Union

from dataflow.context import PipelineContext


# ---------------------------------------------------------------------------
# MapStage
# ---------------------------------------------------------------------------

@dataclass
class MapStage:
    """Apply a function to each record.

    The function can be sync or async.  The stage always returns a
    single-element list (one-to-one mapping).
    """

    fn: Callable[[Any], Any]
    name: str = "map"

    async def process(self, record: Any, ctx: PipelineContext) -> list[Any]:
        result = self.fn(record)
        if asyncio.iscoroutine(result):
            result = await result
        return [result]


# ---------------------------------------------------------------------------
# FilterStage
# ---------------------------------------------------------------------------

@dataclass
class FilterStage:
    """Keep only records that satisfy a predicate.

    Returns a single-element list if the predicate is True, empty otherwise.
    """

    predicate: Callable[[Any], Union[bool, Awaitable[bool]]]
    name: str = "filter"

    async def process(self, record: Any, ctx: PipelineContext) -> list[Any]:
        result = self.predicate(record)
        if asyncio.iscoroutine(result):
            result = await result
        return [record] if result else []


# ---------------------------------------------------------------------------
# FlatMapStage
# ---------------------------------------------------------------------------

@dataclass
class FlatMapStage:
    """Apply a function that returns an iterable, flattening into the stream."""

    fn: Callable[[Any], Any]
    name: str = "flatmap"

    async def process(self, record: Any, ctx: PipelineContext) -> list[Any]:
        result = self.fn(record)
        if asyncio.iscoroutine(result):
            result = await result
        return list(result)


# ---------------------------------------------------------------------------
# BatchStage
# ---------------------------------------------------------------------------

@dataclass
class BatchStage:
    """Collect records into fixed-size batches.

    The stage accumulates records internally and emits a batch (as a list)
    once *size* records have been collected.  A final partial batch is
    emitted when the pipeline finishes (via flush through the pipeline
    runner's record stream ending).

    Because stages process one record at a time, the batch is emitted
    as a single record whose value is a list.
    """

    size: int = 100
    name: str = "batch"
    _buffer: list[Any] = field(default_factory=list, repr=False)

    async def process(self, record: Any, ctx: PipelineContext) -> list[Any]:
        self._buffer.append(record)
        if len(self._buffer) >= self.size:
            batch = self._buffer[: self.size]
            self._buffer = self._buffer[self.size :]
            return [batch]
        return []

    def drain(self) -> list[Any]:
        """Return any remaining buffered records as a final batch."""
        if self._buffer:
            batch = list(self._buffer)
            self._buffer.clear()
            return [batch]
        return []


# ---------------------------------------------------------------------------
# FlattenStage
# ---------------------------------------------------------------------------

@dataclass
class FlattenStage:
    """Flatten a record that is itself an iterable into individual records."""

    name: str = "flatten"

    async def process(self, record: Any, ctx: PipelineContext) -> list[Any]:
        try:
            return list(record)
        except TypeError:
            return [record]


# ---------------------------------------------------------------------------
# WindowStage (tumbling time window)
# ---------------------------------------------------------------------------

@dataclass
class WindowStage:
    """Tumbling time-window aggregation.

    Collects records for *window_seconds* and then emits all records in
    the window as a single list.  Uses wall-clock time.
    """

    window_seconds: float = 5.0
    name: str = "window"
    _buffer: list[Any] = field(default_factory=list, repr=False)
    _window_start: float = field(default=0.0, repr=False)

    async def process(self, record: Any, ctx: PipelineContext) -> list[Any]:
        now = time.monotonic()
        if self._window_start == 0.0:
            self._window_start = now

        self._buffer.append(record)

        if (now - self._window_start) >= self.window_seconds:
            batch = list(self._buffer)
            self._buffer.clear()
            self._window_start = 0.0
            return [batch]
        return []

    def drain(self) -> list[Any]:
        """Emit remaining buffered records."""
        if self._buffer:
            batch = list(self._buffer)
            self._buffer.clear()
            self._window_start = 0.0
            return [batch]
        return []


# ---------------------------------------------------------------------------
# KeyByStage
# ---------------------------------------------------------------------------

@dataclass
class KeyByStage:
    """Attach a key to each record, producing (key, record) tuples."""

    key_fn: Callable[[Any], Any]
    name: str = "key_by"

    async def process(self, record: Any, ctx: PipelineContext) -> list[Any]:
        key = self.key_fn(record)
        return [(key, record)]


# ---------------------------------------------------------------------------
# ChainStage
# ---------------------------------------------------------------------------

@dataclass
class ChainStage:
    """Compose multiple stages sequentially into a single logical stage."""

    children: list[Any]  # list of Stage-compatible objects
    name: str = "chain"

    async def process(self, record: Any, ctx: PipelineContext) -> list[Any]:
        records = [record]
        for stage in self.children:
            next_records: list[Any] = []
            for rec in records:
                result = await stage.process(rec, ctx)
                next_records.extend(result)
            records = next_records
            if not records:
                break
        return records
