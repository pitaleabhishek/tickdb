"""Execute planned queries against compacted chunk storage.

This module is the read-side coordinator for TickDB. Planning and metadata
pruning happen first, then execution reads only the required columns, applies
an optional native numeric mask, rechecks full filter truth in Python, and
finally updates aggregation state.
"""

from __future__ import annotations

from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tickdb.encoding.dictionary import read_dictionary_file
from tickdb.native import (
    DOUBLE_KIND,
    INT64_KIND,
    NativePredicate,
    build_native_mask,
)
from tickdb.query.aggregations import (
    AggregationState,
    finalize_aggregation_states,
    initialize_aggregation_states,
    update_aggregation_states,
)
from tickdb.query.filters import row_matches_filters
from tickdb.query.models import (
    FLOAT_COLUMNS,
    INTEGER_COLUMNS,
    NUMERIC_COLUMNS,
    FilterSpec,
    QueryMetrics,
    QueryPlan,
    QueryResult,
    QuerySpec,
)
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


@dataclass(frozen=True)
class _NativeFilterPushdown:
    column: str
    value_kind: str
    operator: str
    first_value: int | float
    second_value: int | float | None = None
    include_first: bool = True
    include_second: bool = True

    def to_native_predicate(self, *, timestamp_base: int = 0) -> NativePredicate:
        if self.column == "timestamp":
            # The timestamp files store chunk-local offsets, so native timestamp
            # filters must be translated into that same offset space.
            first_value = int(self.first_value) - timestamp_base
            second_value = (
                None
                if self.second_value is None
                else int(self.second_value) - timestamp_base
            )
        else:
            first_value = self.first_value
            second_value = self.second_value

        return NativePredicate(
            value_kind=self.value_kind,
            operator=self.operator,
            first_value=first_value,
            second_value=second_value,
            include_first=self.include_first,
            include_second=self.include_second,
        )


def execute_query(
    root: Path,
    query_spec: QuerySpec,
    *,
    use_native_scan: bool = True,
) -> QueryResult:
    query_plan = build_query_plan(root=root, query_spec=query_spec)
    return execute_query_plan(
        root=root,
        query_plan=query_plan,
        use_native_scan=use_native_scan,
    )


def execute_query_plan(
    root: Path,
    query_plan: QueryPlan,
    *,
    use_native_scan: bool = True,
) -> QueryResult:
    paths = TablePaths(root=root, table=query_plan.table)
    tracker = _MetricTracker.from_query_plan(query_plan)
    native_pushdown = (
        _choose_native_filter_pushdown(query_plan.filters) if use_native_scan else None
    )
    rows = (
        _execute_grouped_query(paths, query_plan, tracker, native_pushdown)
        if query_plan.group_by
        else _execute_ungrouped_query(paths, query_plan, tracker, native_pushdown)
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
    native_pushdown: _NativeFilterPushdown | None,
) -> list[dict[str, Any]]:
    states = initialize_aggregation_states(query_plan.aggregations)

    for _chunk, row_values in _iter_matching_rows(
        paths,
        query_plan,
        tracker,
        native_pushdown,
    ):
        update_aggregation_states(states, row_values)

    return [finalize_aggregation_states(states)]


def _execute_grouped_query(
    paths: TablePaths,
    query_plan: QueryPlan,
    tracker: _MetricTracker,
    native_pushdown: _NativeFilterPushdown | None,
) -> list[dict[str, Any]]:
    grouped_states: dict[tuple[Any, ...], list[AggregationState]] = {}

    for _chunk, row_values in _iter_matching_rows(
        paths,
        query_plan,
        tracker,
        native_pushdown,
    ):
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
    native_pushdown: _NativeFilterPushdown | None,
):
    for chunk in query_plan.candidate_chunks:
        chunk_path = paths.table_root / chunk.path
        tracker.record_scanned_chunk()
        block_index = _load_block_index(chunk, chunk_path)
        tracker.record_total_blocks(len(block_index.blocks))
        # Chunk metadata got us this far; block metadata narrows the exact row
        # ranges worth opening inside the surviving chunk.
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
                native_mask = _build_native_block_mask(
                    readers=readers,
                    row_start=block.row_start,
                    row_stop=block.row_start + block.row_count,
                    native_pushdown=native_pushdown,
                )
                if native_mask is not None:
                    tracker.record_native_scan(block.row_count)

                for row_index in range(block.row_count):
                    # A zero byte means the native numeric predicate already
                    # proved this row cannot match.
                    if native_mask is not None and native_mask[row_index] == 0:
                        continue
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


def _build_native_block_mask(
    readers: dict[str, Any],
    row_start: int,
    row_stop: int,
    native_pushdown: _NativeFilterPushdown | None,
) -> bytes | None:
    if native_pushdown is None:
        return None
    if native_pushdown.column not in readers:
        return None

    reader = readers[native_pushdown.column]
    row_count = row_stop - row_start

    if native_pushdown.column == "timestamp":
        # Native timestamp filters run against the encoded offset buffer rather
        # than reconstructed absolute timestamps.
        raw_bytes = reader.read_range_offset_bytes(row_start, row_stop)
        predicate = native_pushdown.to_native_predicate(timestamp_base=reader.base_value)
        return build_native_mask(raw_bytes, row_count, predicate)

    raw_bytes = reader.read_range_bytes(row_start, row_stop)
    predicate = native_pushdown.to_native_predicate()
    return build_native_mask(raw_bytes, row_count, predicate)


def _load_block_index(chunk: Any, chunk_path: Path) -> BlockIndex:
    block_index_path = chunk_path / "block_index.json"
    if block_index_path.exists():
        return read_block_index(block_index_path)
    # Legacy chunks from before Milestone 10 still execute correctly by
    # treating the whole chunk as one coarse block.
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
        self.native_filter_used = False
        self.native_rows_evaluated = 0

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

    def record_native_scan(self, row_count: int) -> None:
        self.native_filter_used = True
        self.native_rows_evaluated += row_count

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
            native_filter_used=self.native_filter_used,
            native_rows_evaluated=self.native_rows_evaluated,
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


def _choose_native_filter_pushdown(
    filters: list[FilterSpec],
) -> _NativeFilterPushdown | None:
    ordered_columns: list[str] = []
    for filter_spec in filters:
        if (
            filter_spec.column in NUMERIC_COLUMNS
            and filter_spec.column not in ordered_columns
        ):
            ordered_columns.append(filter_spec.column)

    for column in ordered_columns:
        pushdown = _build_native_filter_pushdown_for_column(
            column=column,
            filters=[filter_spec for filter_spec in filters if filter_spec.column == column],
        )
        if pushdown is not None:
            return pushdown
    return None


def _build_native_filter_pushdown_for_column(
    column: str,
    filters: list[FilterSpec],
) -> _NativeFilterPushdown | None:
    value_kind = DOUBLE_KIND if column in FLOAT_COLUMNS else INT64_KIND

    equals = next((filter_spec for filter_spec in filters if filter_spec.operator == "="), None)
    if equals is not None:
        # Equality becomes a closed interval so both Python and C can use the
        # same "between" predicate shape.
        return _NativeFilterPushdown(
            column=column,
            value_kind=value_kind,
            operator="between",
            first_value=equals.value,
            second_value=equals.value,
            include_first=True,
            include_second=True,
        )

    lower_bound: FilterSpec | None = None
    upper_bound: FilterSpec | None = None
    for filter_spec in filters:
        if filter_spec.operator in {">", ">="}:
            lower_bound = _stronger_lower_bound(lower_bound, filter_spec)
        elif filter_spec.operator in {"<", "<="}:
            upper_bound = _stronger_upper_bound(upper_bound, filter_spec)

    if lower_bound is not None and upper_bound is not None:
        return _NativeFilterPushdown(
            column=column,
            value_kind=value_kind,
            operator="between",
            first_value=lower_bound.value,
            second_value=upper_bound.value,
            include_first=lower_bound.operator == ">=",
            include_second=upper_bound.operator == "<=",
        )
    if lower_bound is not None:
        return _NativeFilterPushdown(
            column=column,
            value_kind=value_kind,
            operator=lower_bound.operator,
            first_value=lower_bound.value,
        )
    if upper_bound is not None:
        return _NativeFilterPushdown(
            column=column,
            value_kind=value_kind,
            operator=upper_bound.operator,
            first_value=upper_bound.value,
        )
    return None


def _stronger_lower_bound(
    current: FilterSpec | None,
    candidate: FilterSpec,
) -> FilterSpec:
    if current is None:
        return candidate
    if candidate.value > current.value:
        return candidate
    if candidate.value < current.value:
        return current
    if candidate.operator == ">" and current.operator == ">=":
        return candidate
    return current


def _stronger_upper_bound(
    current: FilterSpec | None,
    candidate: FilterSpec,
) -> FilterSpec:
    if current is None:
        return candidate
    if candidate.value < current.value:
        return candidate
    if candidate.value > current.value:
        return current
    if candidate.operator == "<" and current.operator == "<=":
        return candidate
    return current
