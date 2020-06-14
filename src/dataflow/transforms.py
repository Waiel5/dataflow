"""Higher-level transform stages: validation, coercion, dedup, aggregation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Union

from dataflow.context import PipelineContext
from dataflow.errors import ValidationError


# ---------------------------------------------------------------------------
# SchemaValidator
# ---------------------------------------------------------------------------

@dataclass
class SchemaValidator:
    """Validate records against a simple schema definition.

    The schema is a dict mapping field names to expected types (or a tuple
    of types).  Records missing required fields or with wrong types are
    rejected.

    Example::

        SchemaValidator(schema={"name": str, "age": int}, strict=True)
    """

    schema: dict[str, Union[type, tuple[type, ...]]]
    strict: bool = False  # if True, reject records with extra fields
    name: str = "schema_validator"

    async def process(self, record: Any, ctx: PipelineContext) -> list[Any]:
        if not isinstance(record, dict):
            raise ValidationError(
                f"Expected dict, got {type(record).__name__}",
                stage_name=self.name,
                record=record,
            )

        for field_name, expected_type in self.schema.items():
            if field_name not in record:
                raise ValidationError(
                    f"Missing required field: '{field_name}'",
                    field=field_name,
                    expected=expected_type,
                    got=None,
                    stage_name=self.name,
                    record=record,
                )
            value = record[field_name]
            if not isinstance(value, expected_type):
                raise ValidationError(
                    f"Field '{field_name}': expected {expected_type}, got {type(value).__name__}",
                    field=field_name,
                    expected=expected_type,
                    got=type(value),
                    stage_name=self.name,
                    record=record,
                )

        if self.strict:
            extra = set(record.keys()) - set(self.schema.keys())
            if extra:
                raise ValidationError(
                    f"Unexpected fields: {extra}",
                    stage_name=self.name,
                    record=record,
                )

        return [record]


# ---------------------------------------------------------------------------
# TypeCoercion
# ---------------------------------------------------------------------------

@dataclass
class TypeCoercion:
    """Coerce record fields to specified types.

    Mapping is ``{field_name: target_type}``.  Coercion failures are
    silently skipped unless *strict* is True.

    Example::

        TypeCoercion(mapping={"age": int, "salary": float})
    """

    mapping: dict[str, type]
    strict: bool = False
    name: str = "type_coercion"

    async def process(self, record: Any, ctx: PipelineContext) -> list[Any]:
        if not isinstance(record, dict):
            return [record]

        coerced = dict(record)
        for field_name, target_type in self.mapping.items():
            if field_name in coerced:
                try:
                    coerced[field_name] = target_type(coerced[field_name])
                except (ValueError, TypeError) as exc:
                    if self.strict:
                        raise ValidationError(
                            f"Cannot coerce '{field_name}' to {target_type.__name__}: {exc}",
                            field=field_name,
                            expected=target_type,
                            got=type(coerced[field_name]),
                            stage_name=self.name,
                            record=record,
                        ) from exc
        return [coerced]


# ---------------------------------------------------------------------------
# RenameFields
# ---------------------------------------------------------------------------

@dataclass
class RenameFields:
    """Rename fields in dict records.

    Mapping is ``{old_name: new_name}``.
    """

    mapping: dict[str, str]
    name: str = "rename_fields"

    async def process(self, record: Any, ctx: PipelineContext) -> list[Any]:
        if not isinstance(record, dict):
            return [record]
        renamed = {}
        for k, v in record.items():
            new_key = self.mapping.get(k, k)
            renamed[new_key] = v
        return [renamed]


# ---------------------------------------------------------------------------
# SelectFields
# ---------------------------------------------------------------------------

@dataclass
class SelectFields:
    """Keep only the specified fields from dict records."""

    fields: list[str]
    name: str = "select_fields"

    async def process(self, record: Any, ctx: PipelineContext) -> list[Any]:
        if not isinstance(record, dict):
            return [record]
        return [{k: record[k] for k in self.fields if k in record}]


# ---------------------------------------------------------------------------
# AddFields
# ---------------------------------------------------------------------------

@dataclass
class AddFields:
    """Add computed fields to dict records.

    Mapping is ``{field_name: callable_or_value}``.  If the value is
    callable, it receives the record and should return the field value.
    """

    mapping: dict[str, Any]
    name: str = "add_fields"

    async def process(self, record: Any, ctx: PipelineContext) -> list[Any]:
        if not isinstance(record, dict):
            return [record]
        enriched = dict(record)
        for field_name, value_or_fn in self.mapping.items():
            if callable(value_or_fn):
                enriched[field_name] = value_or_fn(record)
            else:
                enriched[field_name] = value_or_fn
        return [enriched]


# ---------------------------------------------------------------------------
# DeduplicateStage
# ---------------------------------------------------------------------------

@dataclass
class DeduplicateStage:
    """Remove duplicate records based on a key function.

    Uses an in-memory set to track seen keys.  For very large datasets,
    consider using a probabilistic structure externally.
    """

    key_fn: Optional[Callable[[Any], Any]] = None
    name: str = "dedup"
    _seen: set[str] = field(default_factory=set, repr=False)

    async def process(self, record: Any, ctx: PipelineContext) -> list[Any]:
        if self.key_fn is not None:
            raw_key = self.key_fn(record)
        else:
            raw_key = record

        # Hash the key for memory efficiency
        key_str = json.dumps(raw_key, sort_keys=True, default=str)
        key_hash = hashlib.md5(key_str.encode()).hexdigest()

        if key_hash in self._seen:
            return []
        self._seen.add(key_hash)
        return [record]

    def reset(self) -> None:
        """Clear the seen-keys set."""
        self._seen.clear()


# ---------------------------------------------------------------------------
# AggregateStage
# ---------------------------------------------------------------------------

@dataclass
class AggregateStage:
    """Accumulate records and emit an aggregated result.

    This stage buffers ALL records and emits a single summary record
    at the end (the pipeline must drain the remaining buffer).

    The *aggregator* function receives the full list of records and
    should return a list of output records.
    """

    aggregator: Callable[[list[Any]], list[Any]]
    name: str = "aggregate"
    _buffer: list[Any] = field(default_factory=list, repr=False)

    async def process(self, record: Any, ctx: PipelineContext) -> list[Any]:
        self._buffer.append(record)
        return []  # hold records until drain

    def drain(self) -> list[Any]:
        """Invoke the aggregator on all buffered records."""
        if not self._buffer:
            return []
        result = self.aggregator(list(self._buffer))
        self._buffer.clear()
        return result
