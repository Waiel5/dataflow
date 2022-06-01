# dataflow

[![CI](https://github.com/Waiel5/dataflow/actions/workflows/ci.yml/badge.svg)](https://github.com/Waiel5/dataflow/actions/workflows/ci.yml)
[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

**Async data pipeline framework for Python.** Define ETL pipelines as composable stages with backpressure, retry logic, and observability built in.

Think of it as a lightweight, Python-native alternative to Apache Beam — no JVM, no external services, just `asyncio` and clean abstractions.

---

## Architecture

```
                        ┌──────────────────────────────────────────────┐
                        │               Pipeline Runner                │
                        │                                              │
  ┌─────────┐          │  ┌───────┐   ┌───────┐   ┌───────┐          │   ┌──────┐
  │  Source  │─records──│──│Stage 1│──▶│Stage 2│──▶│Stage N│──records──│──▶│ Sink │
  └─────────┘          │  └───┬───┘   └───┬───┘   └───┬───┘          │   └──────┘
                        │      │           │           │              │
                        │      ▼           ▼           ▼              │
                        │  ┌──────────────────────────────────┐       │
                        │  │   Metrics · Retry · Circuit Breaker      │
                        │  └──────────────────────────────────┘       │
                        │                                              │
                        │  ┌──────────────────────────────────┐       │
                        │  │   Backpressure (asyncio.Queue)   │       │
                        │  └──────────────────────────────────┘       │
                        └──────────────────────────────────────────────┘
```

Records flow one at a time from a **Source**, through a chain of **Stages**, and into one or more **Sinks**. An internal `asyncio.Queue` provides backpressure between the producer (source) and the consumer (stage chain + sinks).

## Quickstart

### Install

```bash
pip install git+https://github.com/Waiel5/dataflow.git
```

### Hello Pipeline

```python
import asyncio
from dataflow import Pipeline, IterableSource, MapStage, FilterStage, CallbackSink

async def main():
    pipeline = Pipeline("hello")
    pipeline.add_source(IterableSource([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))
    pipeline.add_stage(FilterStage(lambda x: x % 2 == 0))
    pipeline.add_stage(MapStage(lambda x: x ** 2))
    pipeline.add_sink(CallbackSink(print))
    await pipeline.run()

asyncio.run(main())
# Output: 4  16  36  64  100
```

### CSV to JSON

```python
import asyncio
from dataflow import (
    Pipeline, CSVSource, JSONSink,
    FilterStage, MapStage, TypeCoercion, DeduplicateStage,
)

async def main():
    pipeline = Pipeline("etl-job")
    pipeline.add_source(CSVSource("users.csv"))
    pipeline.add_stage(FilterStage(lambda r: r["status"] == "active"))
    pipeline.add_stage(TypeCoercion(mapping={"age": int}))
    pipeline.add_stage(DeduplicateStage(key_fn=lambda r: r["email"]))
    pipeline.add_sink(JSONSink("active_users.json"))
    ctx = await pipeline.run()
    print(f"Processed {ctx.record_count} records")

asyncio.run(main())
```

### Fluent Builder

```python
pipeline = (
    Pipeline("fluent")
    .add_source(CSVSource("data.csv"))
    .add_stage(FilterStage(lambda r: r["score"] != ""))
    .add_stage(TypeCoercion(mapping={"score": float}))
    .add_stage(MapStage(lambda r: {**r, "grade": "A" if r["score"] >= 90 else "B"}))
    .add_sink(JSONSink("grades.json"))
)
pipeline.run_sync()
```

## Features

### Sources

| Source | Description |
|---|---|
| `IterableSource` | Any Python iterable (lists, generators) |
| `CSVSource` | Read CSV files as dicts |
| `JSONSource` | JSON arrays or newline-delimited JSON |
| `HTTPSource` | Fetch from REST APIs (aiohttp / httpx / urllib) |
| `PollingSource` | Repeatedly poll a callable at intervals |

### Stages

| Stage | Description |
|---|---|
| `MapStage` | One-to-one transform (sync or async) |
| `FilterStage` | Keep/drop records by predicate |
| `FlatMapStage` | One-to-many expansion |
| `BatchStage` | Collect records into fixed-size batches |
| `FlattenStage` | Flatten nested iterables |
| `WindowStage` | Tumbling time-window aggregation |
| `KeyByStage` | Attach a grouping key |
| `ChainStage` | Compose multiple stages as one |

### Transforms

| Transform | Description |
|---|---|
| `SchemaValidator` | Validate record shape and types |
| `TypeCoercion` | Cast fields to target types |
| `RenameFields` | Rename dict keys |
| `SelectFields` | Project specific fields |
| `AddFields` | Add computed or static fields |
| `DeduplicateStage` | Remove duplicates by key |
| `AggregateStage` | Accumulate and reduce |

### Sinks

| Sink | Description |
|---|---|
| `JSONSink` | Write JSON array to file |
| `JSONLSink` | Newline-delimited JSON |
| `CSVSink` | Write dicts as CSV rows |
| `HTTPSink` | POST records to an endpoint |
| `DatabaseSink` | Insert into DB-API 2.0 databases |
| `CallbackSink` | Invoke a function per record |
| `ConsoleSink` | Print to stdout |

### Resilience

```python
from dataflow import RetryStage, RetryPolicy, CircuitBreaker

# Retry with exponential backoff
retried = RetryStage(
    inner=my_stage,
    policy=RetryPolicy(max_attempts=5, base_delay=1.0, jitter=True),
)

# Circuit breaker: trip after 5 failures, recover after 30s
protected = CircuitBreaker(
    inner=my_stage,
    failure_threshold=5,
    recovery_timeout=30.0,
)
```

**Error strategies** at the pipeline level:

```python
pipeline.set_error_strategy("raise")        # default: propagate exceptions
pipeline.set_error_strategy("skip")         # silently drop failing records
pipeline.set_error_strategy("dead_letter")  # route failures to a DLQ
```

### Observability

```python
from dataflow import MetricsCollector, InstrumentedStage

collector = MetricsCollector(pipeline_name="my-pipeline")
collector.start()

pipeline.add_stage(InstrumentedStage(inner=my_stage, collector=collector))

# After the run:
collector.finish()
print(collector.summary())
print(collector.to_prometheus())  # Prometheus text format
```

## Development

```bash
git clone https://github.com/Waiel5/dataflow.git
cd dataflow
pip install -e ".[dev]"
pytest
```

## License

[MIT](LICENSE)
