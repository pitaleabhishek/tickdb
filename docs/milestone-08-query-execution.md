# Milestone 8: Query Execution

This document explains the Milestone 8 implementation: how TickDB executes a previously planned query against compacted chunk storage, how it reads only required columns, how it applies row-level filters, and how it returns deterministic JSON results.

## What Milestone 8 Adds

Before this milestone, TickDB could:

- ingest OHLCV rows into a WAL
- compact those rows into chunked columnar storage
- read fixed-width columns through `mmap`
- plan a query from chunk metadata

What it could not yet do was take that plan and compute a final answer from real chunk data.

After this milestone, TickDB can:

- accept a `query` CLI command
- build a `QuerySpec`
- build a `QueryPlan`
- read only the required columns for selected chunks
- apply row-level filters
- execute:
  - `count`
  - `sum`
  - `avg`
  - `min`
  - `max`
- execute `group by symbol`
- print deterministic JSON results

This milestone executes queries but intentionally keeps richer scan/pruning metrics out of scope.

## Command Surface

Milestone 8 adds:

```bash
tickdb query \
  --table bars \
  --agg avg:close \
  --filter symbol=AAPL
```

The command uses the same grammar already introduced in Milestone 7:

- `--table`
- `--root`
- repeatable `--agg`
- repeatable `--filter`
- repeatable `--group-by`

Examples:

```bash
tickdb query --table bars --agg count
tickdb query --table bars --agg avg:close --filter symbol=AAPL
tickdb query --table bars --agg sum:volume --filter close>31
tickdb query --table bars --agg count --group-by symbol
```

## Why Execution Stays Separate From Planning

Milestone 7 answers:

- which chunks could match?
- which columns are required?

Milestone 8 answers:

- which rows actually match?
- what is the final aggregate result?

That separation matters because metadata pruning is approximate by design. A chunk may survive planning even if only one row inside it matches. The executor is the first place where TickDB narrows the candidate set down to exact matching rows.

## Code Structure

Milestone 8 adds three execution modules.

### `tickdb/query/executor.py`

Responsibilities:

- execute a `QueryPlan`
- load required columns for each candidate chunk
- build row values for execution
- apply filters
- update aggregation state
- return a `QueryResult`

This module orchestrates execution. It does not parse queries and it does not own aggregation math.

### `tickdb/query/filters.py`

Responsibilities:

- evaluate row-level filters against already loaded values

This is separate from the planner so metadata pruning logic and exact row filtering do not get mixed together.

### `tickdb/query/aggregations.py`

Responsibilities:

- initialize aggregation state
- update aggregation state for each matching row
- finalize results into JSON-ready values

This keeps aggregation logic independent from chunk-reading code.

## Execution Flow

The execution path is:

```text
tickdb query
-> build_query_spec(...)
-> build_query_plan(...)
-> execute_query_plan(...)
-> print JSON result
```

Inside `execute_query_plan(...)`, the executor does:

1. iterate over planner-selected chunks
2. load only `required_columns`
3. reconstruct row values
4. apply row-level filters
5. update aggregation state
6. finalize one result row or one row per group

## Reading Required Columns

The executor reads chunk data by column name.

### `symbol`

Read through:

- `symbol.dict.json`
- `symbol.ids.u32`

using the existing dictionary decode helpers.

### `timestamp`

Read through:

- `timestamp.base`
- `timestamp.offsets.i64`

using `TimestampMmapReader`.

### `open/high/low/close`

Read through the corresponding `*.f64` files using `Float64MmapReader`.

### `volume`

Read through `volume.i64` using `Int64MmapReader`.

Important detail:

the executor reads only the columns listed in `QueryPlan.required_columns`. It does not reopen every file in the chunk directory.

## Row-Level Filtering

Planning-level pruning only says a chunk might match. Execution must still test individual rows.

Example:

```bash
tickdb query \
  --table bars \
  --agg sum:volume \
  --filter close>31
```

The planner may keep a chunk because its `close_max` is above `31`. The executor then reads the `close` and `volume` columns for that chunk and checks each row exactly. Only rows whose actual `close` is greater than `31` contribute to the final sum.

This is the main difference between Milestone 7 and Milestone 8.

## Aggregation Semantics

Supported aggregations:

- `count`
- `sum`
- `avg`
- `min`
- `max`

Output keys are deterministic:

- `count`
- `sum_volume`
- `avg_close`
- `min_open`
- `max_volume`

Behavior with no matches:

- `count` => `0`
- `sum` => `0`
- `avg` => `null`
- `min/max` => `null`

## Grouped Results

Initial grouping remains intentionally narrow:

- `group by symbol`

Grouped rows are sorted by the group key so JSON output remains deterministic.

Example:

```json
{
  "table": "bars",
  "filters": [],
  "aggregations": [
    {"function": "count", "column": null}
  ],
  "group_by": ["symbol"],
  "rows": [
    {"symbol": "AAPL", "count": 2},
    {"symbol": "MSFT", "count": 2},
    {"symbol": "NVDA", "count": 2}
  ],
  "selected_chunk_count": 3,
  "result_row_count": 3
}
```

## Result Shape

`tickdb query` prints deterministic JSON with:

- `table`
- `filters`
- `aggregations`
- `group_by`
- `rows`
- `selected_chunk_count`
- `result_row_count`

Ungrouped queries return one result row. Grouped queries return one row per group.

## Tests

Milestone 8 adds integration-oriented tests that build a real compacted table and execute queries against actual on-disk chunk files.

Coverage includes:

- `count` with no filters
- `avg(close)` for a single symbol
- row-level numeric filtering
- `count group by symbol`
- CLI JSON output for `tickdb query`

These tests verify the full path:

```text
WAL -> compaction -> planner -> executor -> JSON result
```

## What This Milestone Does Not Add

This milestone intentionally does not implement:

- detailed execution metrics
- chunk skip/scan reporting in the query output
- native C scan paths
- arbitrary group-by columns
- SQL parsing

Those are deferred so this milestone stays focused on one thing: turning a planned query into a correct result over real compacted storage.
