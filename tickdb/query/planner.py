"""Query planning against chunk metadata."""

from __future__ import annotations

import json
from pathlib import Path

from tickdb.query.models import (
    SCHEMA_ORDER,
    ChunkCandidate,
    FilterSpec,
    QueryPlan,
    QuerySpec,
)
from tickdb.storage.wal import TablePaths


def build_query_plan(root: Path, query_spec: QuerySpec) -> QueryPlan:
    paths = TablePaths(root=root, table=query_spec.table)
    manifest_path = paths.chunks_metadata_path
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"chunk manifest does not exist for table {query_spec.table!r}: {manifest_path}"
        )

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    chunk_entries = [
        ChunkCandidate.from_manifest_entry(entry) for entry in manifest["chunks"]
    ]
    required_columns = calculate_required_columns(query_spec)
    candidate_chunks = [
        chunk for chunk in chunk_entries if chunk_matches_filters(chunk, query_spec.filters)
    ]

    return QueryPlan(
        table=query_spec.table,
        filters=query_spec.filters,
        aggregations=query_spec.aggregations,
        group_by=query_spec.group_by,
        required_columns=required_columns,
        candidate_chunks=candidate_chunks,
        manifest_path=manifest_path,
        total_chunks=len(chunk_entries),
        total_rows=int(manifest["total_rows"]),
    )


def calculate_required_columns(query_spec: QuerySpec) -> list[str]:
    required = {
        filter_spec.column for filter_spec in query_spec.filters
    }
    required.update(
        aggregation.column
        for aggregation in query_spec.aggregations
        if aggregation.column is not None
    )
    required.update(query_spec.group_by)
    return [column for column in SCHEMA_ORDER if column in required]


def chunk_matches_filters(chunk: ChunkCandidate, filters: list[FilterSpec]) -> bool:
    return all(_chunk_matches_filter(chunk, filter_spec) for filter_spec in filters)


def _chunk_matches_filter(chunk: ChunkCandidate, filter_spec: FilterSpec) -> bool:
    if filter_spec.column == "symbol":
        return str(filter_spec.value) in chunk.symbols

    minimum = _metric_value(chunk, f"{filter_spec.column}_min")
    maximum = _metric_value(chunk, f"{filter_spec.column}_max")
    value = filter_spec.value

    if filter_spec.operator == "=":
        return minimum <= value <= maximum
    if filter_spec.operator == ">":
        return maximum > value
    if filter_spec.operator == ">=":
        return maximum >= value
    if filter_spec.operator == "<":
        return minimum < value
    if filter_spec.operator == "<=":
        return minimum <= value

    raise ValueError(f"unsupported filter operator: {filter_spec.operator}")


def _metric_value(chunk: ChunkCandidate, field_name: str) -> int | float:
    if not hasattr(chunk, field_name):
        raise ValueError(f"chunk metadata does not support field {field_name}")
    return getattr(chunk, field_name)
