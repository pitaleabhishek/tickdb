"""Data models for query planning."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCHEMA_ORDER = ["symbol", "timestamp", "open", "high", "low", "close", "volume"]
SCHEMA_COLUMNS = set(SCHEMA_ORDER)
NUMERIC_COLUMNS = {"timestamp", "open", "high", "low", "close", "volume"}
INTEGER_COLUMNS = {"timestamp", "volume"}
FLOAT_COLUMNS = {"open", "high", "low", "close"}
GROUPABLE_COLUMNS = {"symbol"}


@dataclass(frozen=True)
class FilterSpec:
    column: str
    operator: str
    value: str | int | float

    def to_dict(self) -> dict[str, Any]:
        return {
            "column": self.column,
            "operator": self.operator,
            "value": self.value,
        }


@dataclass(frozen=True)
class AggregationSpec:
    function: str
    column: str | None

    @property
    def result_key(self) -> str:
        if self.function == "count":
            return "count"
        return f"{self.function}_{self.column}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "function": self.function,
            "column": self.column,
        }


@dataclass(frozen=True)
class QuerySpec:
    table: str
    filters: list[FilterSpec]
    aggregations: list[AggregationSpec]
    group_by: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "filters": [filter_spec.to_dict() for filter_spec in self.filters],
            "aggregations": [
                aggregation.to_dict() for aggregation in self.aggregations
            ],
            "group_by": self.group_by,
        }


@dataclass(frozen=True)
class ChunkCandidate:
    chunk_id: str
    path: str
    row_count: int
    symbols: list[str]
    timestamp_min: int
    timestamp_max: int
    open_min: float
    open_max: float
    high_min: float
    high_max: float
    low_min: float
    low_max: float
    close_min: float
    close_max: float
    volume_min: int
    volume_max: int

    @classmethod
    def from_manifest_entry(cls, entry: dict[str, Any]) -> ChunkCandidate:
        return cls(
            chunk_id=str(entry["chunk_id"]),
            path=str(entry["path"]),
            row_count=int(entry["row_count"]),
            symbols=[str(symbol) for symbol in entry["symbols"]],
            timestamp_min=int(entry["timestamp_min"]),
            timestamp_max=int(entry["timestamp_max"]),
            open_min=float(entry["open_min"]),
            open_max=float(entry["open_max"]),
            high_min=float(entry["high_min"]),
            high_max=float(entry["high_max"]),
            low_min=float(entry["low_min"]),
            low_max=float(entry["low_max"]),
            close_min=float(entry["close_min"]),
            close_max=float(entry["close_max"]),
            volume_min=int(entry["volume_min"]),
            volume_max=int(entry["volume_max"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "chunk_id": self.chunk_id,
            "path": self.path,
            "row_count": self.row_count,
            "symbols": self.symbols,
            "timestamp_min": self.timestamp_min,
            "timestamp_max": self.timestamp_max,
            "open_min": self.open_min,
            "open_max": self.open_max,
            "high_min": self.high_min,
            "high_max": self.high_max,
            "low_min": self.low_min,
            "low_max": self.low_max,
            "close_min": self.close_min,
            "close_max": self.close_max,
            "volume_min": self.volume_min,
            "volume_max": self.volume_max,
        }


@dataclass(frozen=True)
class QueryPlan:
    table: str
    filters: list[FilterSpec]
    aggregations: list[AggregationSpec]
    group_by: list[str]
    required_columns: list[str]
    candidate_chunks: list[ChunkCandidate]
    manifest_path: Path
    total_chunks: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "filters": [filter_spec.to_dict() for filter_spec in self.filters],
            "aggregations": [
                aggregation.to_dict() for aggregation in self.aggregations
            ],
            "group_by": self.group_by,
            "required_columns": self.required_columns,
            "candidate_chunks": [
                candidate.to_dict() for candidate in self.candidate_chunks
            ],
            "manifest_path": str(self.manifest_path),
            "total_chunks": self.total_chunks,
            "selected_chunk_count": len(self.candidate_chunks),
        }


@dataclass(frozen=True)
class QueryResult:
    table: str
    filters: list[FilterSpec]
    aggregations: list[AggregationSpec]
    group_by: list[str]
    rows: list[dict[str, Any]]
    selected_chunk_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "filters": [filter_spec.to_dict() for filter_spec in self.filters],
            "aggregations": [
                aggregation.to_dict() for aggregation in self.aggregations
            ],
            "group_by": self.group_by,
            "rows": self.rows,
            "selected_chunk_count": self.selected_chunk_count,
            "result_row_count": len(self.rows),
        }
