"""Query execution over compacted chunk storage."""

from __future__ import annotations

from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any

from tickdb.encoding.dictionary import read_dictionary_file
from tickdb.query.aggregations import (
    AggregationState,
    finalize_aggregation_states,
    initialize_aggregation_states,
    update_aggregation_states,
)
from tickdb.query.filters import row_matches_filters
from tickdb.query.models import QueryMetrics, QueryPlan, QueryResult, QuerySpec
from tickdb.query.planner import build_query_plan
from tickdb.query.pruning import metadata_matches_filters
from tickdb.storage.mmap_reader import (
    Float64MmapReader,
    Int64MmapReader,
    TimestampMmapReader,
    UInt32MmapReader,
)
from tickdb.storage.metadata import BlockIndex, BlockMetadata, read_block_index
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
        tracker.record_scanned_chunk()
        block_index = _load_block_index(chunk, chunk_path)
        tracker.record_total_blocks(len(block_index.blocks))
        matching_blocks = [
            block
            for block in block_index.blocks
            if metadata_matches_filters(block, query_plan.filters)
        ]
        if not matching_blocks:
            continue

        with _open_chunk_readers(chunk_path, query_plan.required_columns) as readers:
            for block in matching_blocks:
                tracker.record_scanned_block(block.row_count)
                column_values = _read_block_columns(
                    readers=readers,
                    required_columns=query_plan.required_columns,
                    row_start=block.row_start,
                    row_stop=block.row_start + block.row_count,
                )

                for row_index in range(block.row_count):
                    row_values = {
                        column: column_values[column][row_index]
                        for column in query_plan.required_columns
                    }
                    if row_matches_filters(row_values, query_plan.filters):
                        tracker.record_matched_row()
                        yield chunk, row_values


@contextmanager
def _open_chunk_readers(
    chunk_path: Path,
    required_columns: list[str],
) -> dict[str, Any]:
    with ExitStack() as stack:
        readers = {
            column: _open_column_reader(stack, chunk_path, column)
            for column in required_columns
        }
        yield readers


def _open_column_reader(stack: ExitStack, chunk_path: Path, column: str) -> Any:
    if column == "symbol":
        return stack.enter_context(
            _SymbolRangeReader(
                dictionary_path=chunk_path / "symbol.dict.json",
                ids_path=chunk_path / "symbol.ids.u32",
            )
        )
    if column == "timestamp":
        return stack.enter_context(
            TimestampMmapReader(
                chunk_path / "timestamp.base",
                chunk_path / "timestamp.offsets.i64",
            )
        )
    if column in {"open", "high", "low", "close"}:
        return stack.enter_context(Float64MmapReader(chunk_path / f"{column}.f64"))
    if column == "volume":
        return stack.enter_context(Int64MmapReader(chunk_path / "volume.i64"))
    raise ValueError(f"unsupported column for execution: {column}")


def _read_block_columns(
    readers: dict[str, Any],
    required_columns: list[str],
    row_start: int,
    row_stop: int,
) -> dict[str, list[Any]]:
    return {
        column: readers[column].read_range(row_start, row_stop)
        for column in required_columns
    }


def _load_block_index(chunk: Any, chunk_path: Path) -> BlockIndex:
    block_index_path = chunk_path / "block_index.json"
    if block_index_path.exists():
        return read_block_index(block_index_path)
    return BlockIndex(
        layout="legacy",
        block_size_rows=chunk.row_count,
        blocks=[
            BlockMetadata(
                block_id=0,
                row_start=0,
                row_count=chunk.row_count,
                symbols=list(chunk.symbols),
                timestamp_min=chunk.timestamp_min,
                timestamp_max=chunk.timestamp_max,
                open_min=chunk.open_min,
                open_max=chunk.open_max,
                high_min=chunk.high_min,
                high_max=chunk.high_max,
                low_min=chunk.low_min,
                low_max=chunk.low_max,
                close_min=chunk.close_min,
                close_max=chunk.close_max,
                volume_min=chunk.volume_min,
                volume_max=chunk.volume_max,
            )
        ],
    )


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
        self.total_blocks = 0
        self.scanned_blocks = 0
        self.rows_scanned = 0
        self.rows_matched = 0

    @classmethod
    def from_query_plan(cls, query_plan: QueryPlan) -> _MetricTracker:
        return cls(
            total_chunks=query_plan.total_chunks,
            rows_available=query_plan.total_rows,
            columns_read=list(query_plan.required_columns),
        )

    def record_scanned_chunk(self) -> None:
        self.scanned_chunks += 1

    def record_total_blocks(self, block_count: int) -> None:
        self.total_blocks += block_count

    def record_scanned_block(self, row_count: int) -> None:
        self.scanned_blocks += 1
        self.rows_scanned += row_count

    def record_matched_row(self) -> None:
        self.rows_matched += 1

    def freeze(self) -> QueryMetrics:
        skipped_chunks = self.total_chunks - self.scanned_chunks
        skipped_blocks = self.total_blocks - self.scanned_blocks
        return QueryMetrics(
            total_chunks=self.total_chunks,
            skipped_chunks=skipped_chunks,
            scanned_chunks=self.scanned_chunks,
            total_blocks=self.total_blocks,
            skipped_blocks=skipped_blocks,
            scanned_blocks=self.scanned_blocks,
            rows_available=self.rows_available,
            rows_scanned=self.rows_scanned,
            rows_matched=self.rows_matched,
            columns_read=self.columns_read,
        )


class _SymbolRangeReader:
    def __init__(self, dictionary_path: Path, ids_path: Path) -> None:
        self.dictionary_path = dictionary_path
        self.ids_path = ids_path
        self.dictionary_values: list[str] = []
        self._id_reader = UInt32MmapReader(ids_path)

    def __enter__(self) -> _SymbolRangeReader:
        self.dictionary_values = read_dictionary_file(self.dictionary_path)
        self._id_reader.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._id_reader.__exit__(exc_type, exc, tb)

    def read_range(self, start: int, stop: int) -> list[str]:
        ids = self._id_reader.read_range(start, stop)
        return [self.dictionary_values[int(index)] for index in ids]
