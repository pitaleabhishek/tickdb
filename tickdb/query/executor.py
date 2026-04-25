"""Query execution over compacted chunk storage."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from tickdb.encoding.dictionary import decode_dictionary_values
from tickdb.query.aggregations import (
    AggregationState,
    finalize_aggregation_states,
    initialize_aggregation_states,
    update_aggregation_states,
)
from tickdb.query.filters import row_matches_filters
from tickdb.query.models import QueryPlan, QueryResult, QuerySpec
from tickdb.query.planner import build_query_plan
from tickdb.storage.mmap_reader import (
    Float64MmapReader,
    Int64MmapReader,
    TimestampMmapReader,
)
from tickdb.storage.wal import TablePaths


def execute_query(root: Path, query_spec: QuerySpec) -> QueryResult:
    query_plan = build_query_plan(root=root, query_spec=query_spec)
    return execute_query_plan(root=root, query_plan=query_plan)


def execute_query_plan(root: Path, query_plan: QueryPlan) -> QueryResult:
    paths = TablePaths(root=root, table=query_plan.table)
    rows = (
        _execute_grouped_query(paths, query_plan)
        if query_plan.group_by
        else _execute_ungrouped_query(paths, query_plan)
    )
    return QueryResult(
        table=query_plan.table,
        filters=query_plan.filters,
        aggregations=query_plan.aggregations,
        group_by=query_plan.group_by,
        rows=rows,
        selected_chunk_count=len(query_plan.candidate_chunks),
    )


def _execute_ungrouped_query(
    paths: TablePaths,
    query_plan: QueryPlan,
) -> list[dict[str, Any]]:
    states = initialize_aggregation_states(query_plan.aggregations)

    for _chunk, row_values in _iter_matching_rows(paths, query_plan):
        update_aggregation_states(states, row_values)

    return [finalize_aggregation_states(states)]


def _execute_grouped_query(
    paths: TablePaths,
    query_plan: QueryPlan,
) -> list[dict[str, Any]]:
    grouped_states: dict[tuple[Any, ...], list[AggregationState]] = {}

    for _chunk, row_values in _iter_matching_rows(paths, query_plan):
        group_key = tuple(row_values[column] for column in query_plan.group_by)
        states = grouped_states.setdefault(
            group_key,
            initialize_aggregation_states(query_plan.aggregations),
        )
        update_aggregation_states(states, row_values)

    rows: list[dict[str, Any]] = []
    for group_key in sorted(grouped_states):
        row = {
            column: value for column, value in zip(query_plan.group_by, group_key)
        }
        row.update(finalize_aggregation_states(grouped_states[group_key]))
        rows.append(row)
    return rows


def _iter_matching_rows(
    paths: TablePaths,
    query_plan: QueryPlan,
):
    for chunk in query_plan.candidate_chunks:
        chunk_path = paths.table_root / chunk.path
        column_values = _load_required_columns(
            chunk_path=chunk_path,
            required_columns=query_plan.required_columns,
            expected_row_count=chunk.row_count,
        )

        for row_index in range(chunk.row_count):
            row_values = {
                column: column_values[column][row_index]
                for column in query_plan.required_columns
            }
            if row_matches_filters(row_values, query_plan.filters):
                yield chunk, row_values


def _load_required_columns(
    chunk_path: Path,
    required_columns: list[str],
    expected_row_count: int,
) -> dict[str, list[Any]]:
    return {
        column: _read_column(chunk_path, column, expected_row_count)
        for column in required_columns
    }


def _read_column(
    chunk_path: Path,
    column: str,
    expected_row_count: int,
) -> list[Any]:
    if column == "symbol":
        values = decode_dictionary_values(
            chunk_path / "symbol.dict.json",
            chunk_path / "symbol.ids.u32",
        )
    elif column == "timestamp":
        with TimestampMmapReader(
            chunk_path / "timestamp.base",
            chunk_path / "timestamp.offsets.i64",
        ) as reader:
            values = reader.read_all()
    elif column in {"open", "high", "low", "close"}:
        with Float64MmapReader(chunk_path / f"{column}.f64") as reader:
            values = reader.read_all()
    elif column == "volume":
        with Int64MmapReader(chunk_path / "volume.i64") as reader:
            values = reader.read_all()
    else:
        raise ValueError(f"unsupported column for execution: {column}")

    if len(values) != expected_row_count:
        raise ValueError(
            f"column {column!r} in {chunk_path} has {len(values)} rows; expected {expected_row_count}"
        )
    return values
