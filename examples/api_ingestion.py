#!/usr/bin/env python3
"""Example: Poll a REST API and stream records to JSONL.

This pipeline polls the JSONPlaceholder API every 5 seconds, enriches
each post with a word-count field, and appends the results to a
newline-delimited JSON file.

Usage:
    python api_ingestion.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from dataflow import (
    Pipeline,
    HTTPSource,
    JSONLSink,
    MapStage,
    FilterStage,
    AddFields,
    SchemaValidator,
    RetryStage,
    RetryPolicy,
    MetricsCollector,
    InstrumentedStage,
)


def word_count(record: dict) -> int:
    """Count words in the post body."""
    return len(record.get("body", "").split())


async def main() -> None:
    collector = MetricsCollector(pipeline_name="api-ingestion")
    collector.start()

    pipeline = Pipeline("api-ingestion")

    # Source: fetch posts from JSONPlaceholder
    pipeline.add_source(
        HTTPSource(url="https://jsonplaceholder.typicode.com/posts")
    )

    # Validate the shape of each record
    validator = SchemaValidator(
        schema={"userId": int, "id": int, "title": str, "body": str}
    )
    pipeline.add_stage(
        InstrumentedStage(
            inner=RetryStage(inner=validator, policy=RetryPolicy(max_attempts=2, base_delay=0.1)),
            collector=collector,
        )
    )

    # Only keep posts from user 1
    pipeline.add_stage(
        InstrumentedStage(
            inner=FilterStage(lambda r: r["userId"] == 1),
            collector=collector,
        )
    )

    # Enrich with word count
    pipeline.add_stage(
        InstrumentedStage(
            inner=AddFields(mapping={"word_count": word_count}),
            collector=collector,
        )
    )

    # Transform: create a summary view
    pipeline.add_stage(
        MapStage(
            lambda r: {
                "id": r["id"],
                "title": r["title"],
                "word_count": r["word_count"],
                "preview": r["body"][:80].replace("\n", " "),
            }
        )
    )

    pipeline.add_sink(JSONLSink("output/posts.jsonl"))

    # Use dead-letter strategy so bad records don't crash the pipeline
    pipeline.set_error_strategy("dead_letter")

    ctx = await pipeline.run()
    collector.finish()

    print(f"\nProcessed {ctx.record_count} records")
    print(f"\nMetrics summary:")
    for key, value in collector.summary().items():
        print(f"  {key}: {value}")

    if pipeline.dead_letter_queue:
        print(f"\nDead-letter queue: {len(pipeline.dead_letter_queue)} records")


if __name__ == "__main__":
    asyncio.run(main())
