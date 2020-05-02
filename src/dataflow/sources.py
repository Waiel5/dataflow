"""Built-in data sources: CSV, JSON, HTTP, iterable, and polling."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Iterable, Optional

from dataflow.context import PipelineContext
from dataflow.errors import SourceError


# ---------------------------------------------------------------------------
# IterableSource
# ---------------------------------------------------------------------------

@dataclass
class IterableSource:
    """Emit records from any Python iterable (useful for testing)."""

    data: Iterable[Any]

    async def read(self, ctx: PipelineContext) -> AsyncIterator[Any]:
        for item in self.data:
            yield item


# ---------------------------------------------------------------------------
# CSVSource
# ---------------------------------------------------------------------------

@dataclass
class CSVSource:
    """Read rows from a CSV file as dictionaries.

    Parameters:
        path: Filesystem path to the CSV file.
        delimiter: Column separator (default ``','``).
        encoding: File encoding (default ``'utf-8'``).
    """

    path: str
    delimiter: str = ","
    encoding: str = "utf-8"

    async def read(self, ctx: PipelineContext) -> AsyncIterator[dict[str, str]]:
        resolved = Path(self.path).expanduser().resolve()
        if not resolved.is_file():
            raise SourceError(f"CSV file not found: {resolved}")

        ctx.logger.info("Reading CSV from %s", resolved)

        # Read in a thread to avoid blocking the event loop
        loop = asyncio.get_running_loop()
        content = await loop.run_in_executor(
            None, lambda: resolved.read_text(encoding=self.encoding)
        )

        reader = csv.DictReader(io.StringIO(content), delimiter=self.delimiter)
        for row in reader:
            yield dict(row)


# ---------------------------------------------------------------------------
# JSONSource
# ---------------------------------------------------------------------------

@dataclass
class JSONSource:
    """Read records from a JSON file.

    The file should contain either a JSON array or newline-delimited JSON
    (one object per line).
    """

    path: str
    encoding: str = "utf-8"
    jsonl: bool = False

    async def read(self, ctx: PipelineContext) -> AsyncIterator[Any]:
        resolved = Path(self.path).expanduser().resolve()
        if not resolved.is_file():
            raise SourceError(f"JSON file not found: {resolved}")

        ctx.logger.info("Reading JSON from %s", resolved)
        loop = asyncio.get_running_loop()
        content = await loop.run_in_executor(
            None, lambda: resolved.read_text(encoding=self.encoding)
        )

        if self.jsonl:
            for line in content.strip().splitlines():
                line = line.strip()
                if line:
                    yield json.loads(line)
        else:
            data = json.loads(content)
            if isinstance(data, list):
                for item in data:
                    yield item
            else:
                yield data


# ---------------------------------------------------------------------------
# HTTPSource
# ---------------------------------------------------------------------------

@dataclass
class HTTPSource:
    """Fetch records from an HTTP endpoint.

    Requires the ``aiohttp`` or ``httpx`` library at runtime.  Falls back
    to ``urllib`` for simple GETs if neither is installed.

    The response is expected to be JSON.  If the response is a list, each
    item becomes a separate record; otherwise the entire response is a
    single record.
    """

    url: str
    method: str = "GET"
    headers: dict[str, str] = field(default_factory=dict)
    params: dict[str, str] = field(default_factory=dict)
    timeout: float = 30.0

    async def read(self, ctx: PipelineContext) -> AsyncIterator[Any]:
        ctx.logger.info("Fetching %s %s", self.method, self.url)
        data = await self._fetch()

        if isinstance(data, list):
            for item in data:
                yield item
        else:
            yield data

    async def _fetch(self) -> Any:
        # Try aiohttp first, then httpx, then stdlib
        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                async with session.request(
                    self.method,
                    self.url,
                    headers=self.headers,
                    params=self.params,
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                ) as resp:
                    resp.raise_for_status()
                    return await resp.json()
        except ImportError:
            pass

        try:
            import httpx

            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.request(
                    self.method, self.url, headers=self.headers, params=self.params
                )
                resp.raise_for_status()
                return resp.json()
        except ImportError:
            pass

        # Fallback to urllib (sync, run in executor)
        import urllib.request

        loop = asyncio.get_running_loop()

        def _sync_fetch() -> Any:
            req = urllib.request.Request(self.url, headers=self.headers, method=self.method)
            with urllib.request.urlopen(req, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8"))

        return await loop.run_in_executor(None, _sync_fetch)


# ---------------------------------------------------------------------------
# PollingSource
# ---------------------------------------------------------------------------

@dataclass
class PollingSource:
    """Repeatedly poll a callable at a fixed interval.

    The callable should return an iterable of records.  The source will
    keep polling until ``max_polls`` is reached or the pipeline is
    cancelled.
    """

    poll_fn: Callable[[], Any]
    interval_seconds: float = 10.0
    max_polls: int = 0  # 0 = unlimited

    async def read(self, ctx: PipelineContext) -> AsyncIterator[Any]:
        polls = 0
        while True:
            polls += 1
            ctx.logger.debug("Poll #%d", polls)

            result = self.poll_fn()
            if asyncio.iscoroutine(result):
                result = await result

            if result is not None:
                try:
                    for item in result:
                        yield item
                except TypeError:
                    yield result

            if 0 < self.max_polls <= polls:
                break

            await asyncio.sleep(self.interval_seconds)
