"""dataflow - Async data pipeline framework for Python.

Composable ETL stages with backpressure, retry, and observability.
"""

__version__ = "0.1.0"

# Core
from dataflow.pipeline import Pipeline, Source, Stage, Sink
from dataflow.context import PipelineContext

# Built-in stages
from dataflow.stages import (
    MapStage,
    FilterStage,
    FlatMapStage,
    BatchStage,
    FlattenStage,
    WindowStage,
    KeyByStage,
    ChainStage,
)

# Sources
from dataflow.sources import (
    IterableSource,
    CSVSource,
    JSONSource,
    HTTPSource,
    PollingSource,
)

# Sinks
from dataflow.sinks import (
    JSONSink,
    JSONLSink,
    CSVSink,
    HTTPSink,
    CallbackSink,
    ConsoleSink,
    DatabaseSink,
)

# Transforms
from dataflow.transforms import (
    SchemaValidator,
    TypeCoercion,
    RenameFields,
    SelectFields,
    AddFields,
    DeduplicateStage,
    AggregateStage,
)

# Retry / resilience
from dataflow.retry import (
    RetryPolicy,
    RetryStage,
    CircuitBreaker,
    DeadLetterQueue,
    retry_async,
    with_retry,
)

# Metrics
from dataflow.metrics import (
    MetricsCollector,
    InstrumentedStage,
    Counter,
    Gauge,
    Histogram,
)

# Errors
from dataflow.errors import (
    DataflowError,
    PipelineError,
    StageError,
    SourceError,
    SinkError,
    RetryExhaustedError,
    CircuitOpenError,
    ValidationError,
    BackpressureError,
    ConfigurationError,
)

__all__ = [
    # Core
    "Pipeline",
    "PipelineContext",
    "Source",
    "Stage",
    "Sink",
    # Stages
    "MapStage",
    "FilterStage",
    "FlatMapStage",
    "BatchStage",
    "FlattenStage",
    "WindowStage",
    "KeyByStage",
    "ChainStage",
    # Sources
    "IterableSource",
    "CSVSource",
    "JSONSource",
    "HTTPSource",
    "PollingSource",
    # Sinks
    "JSONSink",
    "JSONLSink",
    "CSVSink",
    "HTTPSink",
    "CallbackSink",
    "ConsoleSink",
    "DatabaseSink",
    # Transforms
    "SchemaValidator",
    "TypeCoercion",
    "RenameFields",
    "SelectFields",
    "AddFields",
    "DeduplicateStage",
    "AggregateStage",
    # Retry
    "RetryPolicy",
    "RetryStage",
    "CircuitBreaker",
    "DeadLetterQueue",
    "retry_async",
    "with_retry",
    # Metrics
    "MetricsCollector",
    "InstrumentedStage",
    "Counter",
    "Gauge",
    "Histogram",
    # Errors
    "DataflowError",
    "PipelineError",
    "StageError",
    "SourceError",
    "SinkError",
    "RetryExhaustedError",
    "CircuitOpenError",
    "ValidationError",
    "BackpressureError",
    "ConfigurationError",
]
