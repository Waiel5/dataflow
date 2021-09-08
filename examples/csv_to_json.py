#!/usr/bin/env python3
"""Example: Convert a CSV file to JSON with filtering and transformation.

Usage:
    python csv_to_json.py input.csv output.json

This pipeline reads a CSV, filters for active users, normalises the
email field to lowercase, and writes the result as a JSON array.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# Ensure the local package is importable when running from the repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from dataflow import (
    Pipeline,
    CSVSource,
    JSONSink,
    FilterStage,
    MapStage,
    TypeCoercion,
    SelectFields,
    DeduplicateStage,
)


def normalise_email(record: dict) -> dict:
    """Lowercase the email field if present."""
    if "email" in record:
        record = {**record, "email": record["email"].strip().lower()}
    return record


async def main(input_csv: str, output_json: str) -> None:
    pipeline = Pipeline("csv-to-json")

    pipeline.add_source(CSVSource(input_csv))

    # Drop rows where status is not 'active'
    pipeline.add_stage(
        FilterStage(lambda row: row.get("status", "").lower() == "active")
    )

    # Coerce numeric fields
    pipeline.add_stage(
        TypeCoercion(mapping={"age": int, "salary": float})
    )

    # Normalise emails
    pipeline.add_stage(MapStage(normalise_email))

    # Remove duplicates by email
    pipeline.add_stage(
        DeduplicateStage(key_fn=lambda r: r.get("email"))
    )

    # Keep only the fields we care about
    pipeline.add_stage(
        SelectFields(fields=["name", "email", "age", "salary"])
    )

    pipeline.add_sink(JSONSink(output_json))

    ctx = await pipeline.run()
    print(f"Done. {ctx.record_count} records written to {output_json}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__.strip())
        sys.exit(1)
    asyncio.run(main(sys.argv[1], sys.argv[2]))
