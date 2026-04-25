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
from tickdb.query.models import QueryMetrics, QueryPlan, QueryResult, QuerySpec
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
    tracker = _MetricTracker.from_query_plan(query_plan)
    rows = (
        _execute_grouped_query(paths, query_plan, tracker)
        if query_plan.group_by
        else _execute_ungrouped_query(paths, query_plan, tracker)
    )
    return QueryResult(
        table=query_plan.table,
        filters=query_plan.filters,
        aggregations=query_plan.aggregations,
        group_by=query_plan.group_by,
        rows=rows,
        metrics=tracker.freeze(),
    )


def _execute_ungrouped_query(
    paths: TablePaths,
    query_plan: QueryPlan,
    tracker: _MetricTracker,
) -> list[dict[str, Any]]:
    states = initialize_aggregation_states(query_plan.aggregations)

    for _chunk, row_values in _iter_matching_rows(paths, query_plan, tracker):
        update_aggregation_states(states, row_values)

    return [finalize_aggregation_states(states)]


def _execute_grouped_query(
    paths: TablePaths,
    query_plan: QueryPlan,
    tracker: _MetricTracker,
) -> list[dict[str, Any]]:
    grouped_states: dict[tuple[Any, ...], list[AggregationState]] = {}

    for _chunk, row_values in _iter_matching_rows(paths, query_plan, tracker):
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
    tracker: _MetricTracker,
):
    for chunk in query_plan.candidate_chunks:
        chunk_path = paths.table_root / chunk.path
        tracker.record_scanned_chunk(chunk.row_count)
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
                tracker.record_matched_row()
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


class _MetricTracker:
    def __init__(
        self,
        total_chunks: int,
        rows_available: int,
        columns_read: list[str],
    ) -> None:
        self.total_chunks = total_chunks
        self.rows_available = rows_available
        self.columns_read = columns_read
        self.scanned_chunks = 0
        self.rows_scanned = 0
        self.rows_matched = 0

    @classmethod
    def from_query_plan(cls, query_plan: QueryPlan) -> _MetricTracker:
        return cls(
            total_chunks=query_plan.total_chunks,
            rows_available=query_plan.total_rows,
            columns_read=list(query_plan.required_columns),
        )

    def record_scanned_chunk(self, row_count: int) -> None:
        self.scanned_chunks += 1
        self.rows_scanned += row_count

    def record_matched_row(self) -> None:
        self.rows_matched += 1

    def freeze(self) -> QueryMetrics:
        skipped_chunks = self.total_chunks - self.scanned_chunks
        return QueryMetrics(
            total_chunks=self.total_chunks,
            skipped_chunks=skipped_chunks,
            scanned_chunks=self.scanned_chunks,
            rows_available=self.rows_available,
            rows_scanned=self.rows_scanned,
            rows_matched=self.rows_matched,
            columns_read=self.columns_read,
        )
