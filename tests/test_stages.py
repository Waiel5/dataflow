"""Tests for built-in stages and transform stages."""

from __future__ import annotations

import pytest
from dataflow import (
    MapStage,
    FilterStage,
    FlatMapStage,
    BatchStage,
    FlattenStage,
    KeyByStage,
    ChainStage,
    SchemaValidator,
    TypeCoercion,
    RenameFields,
    SelectFields,
    AddFields,
    DeduplicateStage,
    PipelineContext,
    ValidationError,
)


@pytest.fixture
def ctx():
    return PipelineContext(pipeline_name="test")


# ---------------------------------------------------------------------------
# Basic stages
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_map_stage(ctx):
    stage = MapStage(lambda x: x * 3)
    assert await stage.process(5, ctx) == [15]


@pytest.mark.asyncio
async def test_map_stage_async(ctx):
    async def double(x):
        return x * 2

    stage = MapStage(double)
    assert await stage.process(7, ctx) == [14]


@pytest.mark.asyncio
async def test_filter_pass(ctx):
    stage = FilterStage(lambda x: x > 3)
    assert await stage.process(5, ctx) == [5]


@pytest.mark.asyncio
async def test_filter_drop(ctx):
    stage = FilterStage(lambda x: x > 3)
    assert await stage.process(1, ctx) == []


@pytest.mark.asyncio
async def test_flatmap(ctx):
    stage = FlatMapStage(lambda x: [x, x + 1])
    assert await stage.process(10, ctx) == [10, 11]


@pytest.mark.asyncio
async def test_flatten(ctx):
    stage = FlattenStage()
    assert await stage.process([1, 2, 3], ctx) == [1, 2, 3]


@pytest.mark.asyncio
async def test_flatten_non_iterable(ctx):
    stage = FlattenStage()
    assert await stage.process(42, ctx) == [42]


@pytest.mark.asyncio
async def test_key_by(ctx):
    stage = KeyByStage(lambda r: r["id"])
    result = await stage.process({"id": "a", "value": 1}, ctx)
    assert result == [("a", {"id": "a", "value": 1})]


# ---------------------------------------------------------------------------
# BatchStage
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_batch_stage_full_batch(ctx):
    stage = BatchStage(size=3)
    assert await stage.process(1, ctx) == []
    assert await stage.process(2, ctx) == []
    result = await stage.process(3, ctx)
    assert result == [[1, 2, 3]]


@pytest.mark.asyncio
async def test_batch_stage_drain(ctx):
    stage = BatchStage(size=5)
    await stage.process(1, ctx)
    await stage.process(2, ctx)
    remaining = stage.drain()
    assert remaining == [[1, 2]]


# ---------------------------------------------------------------------------
# ChainStage
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_chain_stage(ctx):
    chain = ChainStage(
        children=[
            FilterStage(lambda x: x > 0),
            MapStage(lambda x: x * 10),
        ]
    )
    assert await chain.process(3, ctx) == [30]
    assert await chain.process(-1, ctx) == []


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_schema_validator_pass(ctx):
    validator = SchemaValidator(schema={"name": str, "age": int})
    record = {"name": "alice", "age": 30}
    assert await validator.process(record, ctx) == [record]


@pytest.mark.asyncio
async def test_schema_validator_missing_field(ctx):
    validator = SchemaValidator(schema={"name": str, "age": int})
    with pytest.raises(ValidationError, match="Missing required field"):
        await validator.process({"name": "alice"}, ctx)


@pytest.mark.asyncio
async def test_schema_validator_wrong_type(ctx):
    validator = SchemaValidator(schema={"name": str, "age": int})
    with pytest.raises(ValidationError, match="expected"):
        await validator.process({"name": "alice", "age": "thirty"}, ctx)


@pytest.mark.asyncio
async def test_schema_validator_strict_extra_fields(ctx):
    validator = SchemaValidator(schema={"name": str}, strict=True)
    with pytest.raises(ValidationError, match="Unexpected fields"):
        await validator.process({"name": "alice", "extra": 1}, ctx)


@pytest.mark.asyncio
async def test_schema_validator_non_dict(ctx):
    validator = SchemaValidator(schema={"x": int})
    with pytest.raises(ValidationError, match="Expected dict"):
        await validator.process("not a dict", ctx)


# ---------------------------------------------------------------------------
# Type coercion
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_type_coercion(ctx):
    coerce = TypeCoercion(mapping={"age": int, "salary": float})
    record = {"age": "30", "salary": "55000.50", "name": "bob"}
    result = await coerce.process(record, ctx)
    assert result == [{"age": 30, "salary": 55000.50, "name": "bob"}]


@pytest.mark.asyncio
async def test_type_coercion_strict_failure(ctx):
    coerce = TypeCoercion(mapping={"age": int}, strict=True)
    with pytest.raises(ValidationError, match="Cannot coerce"):
        await coerce.process({"age": "not_a_number"}, ctx)


@pytest.mark.asyncio
async def test_type_coercion_lenient(ctx):
    coerce = TypeCoercion(mapping={"age": int}, strict=False)
    result = await coerce.process({"age": "not_a_number"}, ctx)
    assert result == [{"age": "not_a_number"}]


# ---------------------------------------------------------------------------
# Field operations
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rename_fields(ctx):
    stage = RenameFields(mapping={"old_name": "new_name"})
    result = await stage.process({"old_name": "value", "keep": 1}, ctx)
    assert result == [{"new_name": "value", "keep": 1}]


@pytest.mark.asyncio
async def test_select_fields(ctx):
    stage = SelectFields(fields=["a", "c"])
    result = await stage.process({"a": 1, "b": 2, "c": 3}, ctx)
    assert result == [{"a": 1, "c": 3}]


@pytest.mark.asyncio
async def test_add_fields_static(ctx):
    stage = AddFields(mapping={"source": "manual"})
    result = await stage.process({"x": 1}, ctx)
    assert result == [{"x": 1, "source": "manual"}]


@pytest.mark.asyncio
async def test_add_fields_callable(ctx):
    stage = AddFields(mapping={"doubled": lambda r: r["x"] * 2})
    result = await stage.process({"x": 5}, ctx)
    assert result == [{"x": 5, "doubled": 10}]


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dedup_default_key(ctx):
    stage = DeduplicateStage()
    assert await stage.process(1, ctx) == [1]
    assert await stage.process(2, ctx) == [2]
    assert await stage.process(1, ctx) == []  # duplicate
    assert await stage.process(3, ctx) == [3]


@pytest.mark.asyncio
async def test_dedup_custom_key(ctx):
    stage = DeduplicateStage(key_fn=lambda r: r["id"])
    assert await stage.process({"id": "a", "v": 1}, ctx) == [{"id": "a", "v": 1}]
    assert await stage.process({"id": "a", "v": 2}, ctx) == []  # same id
    assert await stage.process({"id": "b", "v": 3}, ctx) == [{"id": "b", "v": 3}]


@pytest.mark.asyncio
async def test_dedup_reset(ctx):
    stage = DeduplicateStage()
    await stage.process(1, ctx)
    assert await stage.process(1, ctx) == []
    stage.reset()
    assert await stage.process(1, ctx) == [1]
