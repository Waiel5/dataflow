"""Built-in data sinks: file, JSON, CSV, HTTP, callback, and database."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from dataflow.context import PipelineContext
from dataflow.errors import SinkError


# ---------------------------------------------------------------------------
# JSONSink
# ---------------------------------------------------------------------------

@dataclass
class JSONSink:
    """Write records as a JSON array to a file.

    Records are buffered in memory and flushed on pipeline completion.
    For large datasets consider ``JSONLSink`` instead.
    """

    path: str
    encoding: str = "utf-8"
    indent: int = 2
    _buffer: list[Any] = field(default_factory=list, repr=False)

    async def write(self, record: Any, ctx: PipelineContext) -> None:
        self._buffer.append(record)

    async def flush(self, ctx: PipelineContext) -> None:
        resolved = Path(self.path).expanduser().resolve()
        resolved.parent.mkdir(parents=True, exist_ok=True)
        ctx.logger.info("Writing %d records to %s", len(self._buffer), resolved)

        loop = asyncio.get_running_loop()
        content = json.dumps(self._buffer, indent=self.indent, default=str)
        await loop.run_in_executor(
            None, lambda: resolved.write_text(content, encoding=self.encoding)
        )
        self._buffer.clear()


# ---------------------------------------------------------------------------
# JSONLSink (newline-delimited JSON)
# ---------------------------------------------------------------------------

@dataclass
class JSONLSink:
    """Append records as newline-delimited JSON (one object per line)."""

    path: str
    encoding: str = "utf-8"
    _buffer: list[str] = field(default_factory=list, repr=False)

    async def write(self, record: Any, ctx: PipelineContext) -> None:
        self._buffer.append(json.dumps(record, default=str))

    async def flush(self, ctx: PipelineContext) -> None:
        resolved = Path(self.path).expanduser().resolve()
        resolved.parent.mkdir(parents=True, exist_ok=True)
        ctx.logger.info("Writing %d lines to %s", len(self._buffer), resolved)

        loop = asyncio.get_running_loop()
        content = "\n".join(self._buffer) + "\n"
        await loop.run_in_executor(
            None, lambda: resolved.write_text(content, encoding=self.encoding)
        )
        self._buffer.clear()


# ---------------------------------------------------------------------------
# CSVSink
# ---------------------------------------------------------------------------

@dataclass
class CSVSink:
    """Write dictionaries as rows to a CSV file.

    Field names are inferred from the first record.
    """

    path: str
    delimiter: str = ","
    encoding: str = "utf-8"
    _buffer: list[dict[str, Any]] = field(default_factory=list, repr=False)

    async def write(self, record: Any, ctx: PipelineContext) -> None:
        if not isinstance(record, dict):
            raise SinkError(f"CSVSink expects dict records, got {type(record).__name__}")
        self._buffer.append(record)

    async def flush(self, ctx: PipelineContext) -> None:
        if not self._buffer:
            return

        resolved = Path(self.path).expanduser().resolve()
        resolved.parent.mkdir(parents=True, exist_ok=True)
        ctx.logger.info("Writing %d rows to %s", len(self._buffer), resolved)

        fieldnames = list(self._buffer[0].keys())
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames, delimiter=self.delimiter)
        writer.writeheader()
        writer.writerows(self._buffer)

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, lambda: resolved.write_text(output.getvalue(), encoding=self.encoding)
        )
        self._buffer.clear()


# ---------------------------------------------------------------------------
# HTTPSink
# ---------------------------------------------------------------------------

@dataclass
class HTTPSink:
    """POST each record as JSON to an HTTP endpoint."""

    url: str
    headers: dict[str, str] = field(default_factory=dict)
    batch_size: int = 1
    timeout: float = 30.0
    _buffer: list[Any] = field(default_factory=list, repr=False)

    async def write(self, record: Any, ctx: PipelineContext) -> None:
        self._buffer.append(record)
        if len(self._buffer) >= self.batch_size:
            await self._send(ctx)

    async def flush(self, ctx: PipelineContext) -> None:
        if self._buffer:
            await self._send(ctx)

    async def _send(self, ctx: PipelineContext) -> None:
        payload = self._buffer if len(self._buffer) > 1 else self._buffer[0]
        self._buffer.clear()

        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.url,
                    json=payload,
                    headers=self.headers,
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                ) as resp:
                    resp.raise_for_status()
            return
        except ImportError:
            pass

        try:
            import httpx

            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(self.url, json=payload, headers=self.headers)
                resp.raise_for_status()
            return
        except ImportError:
            pass

        # Fallback to urllib
        import urllib.request

        loop = asyncio.get_running_loop()

        def _sync_post() -> None:
            data = json.dumps(payload, default=str).encode("utf-8")
            headers = {**self.headers, "Content-Type": "application/json"}
            req = urllib.request.Request(self.url, data=data, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=self.timeout):
                pass

        await loop.run_in_executor(None, _sync_post)


# ---------------------------------------------------------------------------
# CallbackSink
# ---------------------------------------------------------------------------

@dataclass
class CallbackSink:
    """Invoke a user-supplied callback for each record.

    The callback can be sync or async.
    """

    fn: Callable[[Any], Any]

    async def write(self, record: Any, ctx: PipelineContext) -> None:
        result = self.fn(record)
        if asyncio.iscoroutine(result):
            await result

    async def flush(self, ctx: PipelineContext) -> None:
        pass  # nothing to flush


# ---------------------------------------------------------------------------
# ConsoleSink
# ---------------------------------------------------------------------------

@dataclass
class ConsoleSink:
    """Print each record to stdout (useful for debugging)."""

    prefix: str = ""

    async def write(self, record: Any, ctx: PipelineContext) -> None:
        line = f"{self.prefix}{record}" if self.prefix else str(record)
        print(line)

    async def flush(self, ctx: PipelineContext) -> None:
        pass


# ---------------------------------------------------------------------------
# DatabaseSink
# ---------------------------------------------------------------------------

@dataclass
class DatabaseSink:
    """Insert records into a database table using DB-API 2.0 compatible connections.

    Records should be dicts whose keys match column names.
    Rows are buffered and inserted in batches.
    """

    table: str
    connection_factory: Callable[[], Any]
    batch_size: int = 500
    _buffer: list[dict[str, Any]] = field(default_factory=list, repr=False)

    async def write(self, record: Any, ctx: PipelineContext) -> None:
        if not isinstance(record, dict):
            raise SinkError(f"DatabaseSink expects dict records, got {type(record).__name__}")
        self._buffer.append(record)
        if len(self._buffer) >= self.batch_size:
            await self._flush_batch(ctx)

    async def flush(self, ctx: PipelineContext) -> None:
        if self._buffer:
            await self._flush_batch(ctx)

    async def _flush_batch(self, ctx: PipelineContext) -> None:
        if not self._buffer:
            return

        rows = list(self._buffer)
        self._buffer.clear()
        columns = list(rows[0].keys())
        placeholders = ", ".join(["?"] * len(columns))
        col_str = ", ".join(columns)
        sql = f"INSERT INTO {self.table} ({col_str}) VALUES ({placeholders})"

        loop = asyncio.get_running_loop()

        def _insert() -> None:
            conn = self.connection_factory()
            cursor = conn.cursor()
            try:
                cursor.executemany(sql, [tuple(r.get(c) for c in columns) for r in rows])
                conn.commit()
            finally:
                cursor.close()

        await loop.run_in_executor(None, _insert)
        ctx.logger.debug("Inserted %d rows into %s", len(rows), self.table)
