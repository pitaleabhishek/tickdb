# Milestone 9: Pruning Metrics

This document explains the Milestone 9 implementation: how TickDB now reports pruning and scan-cost metrics alongside query results, and why the metrics are structured the way they are.

## What Milestone 9 Adds

Before this milestone, `tickdb query` returned:

- query metadata
- result rows
- `result_row_count`

What it did not yet expose was how much work the query engine actually avoided or performed.

After this milestone, `tickdb query` also returns a nested `metrics` object with:

- `total_chunks`
- `skipped_chunks`
- `scanned_chunks`
- `rows_available`
- `rows_scanned`
- `rows_matched`
- `columns_read`
- `column_count`
- `pruning_rate`

## Why The Metrics Are Nested

The metrics live under a dedicated `metrics` key rather than being mixed into the top level.

That matters because:

- correctness output (`rows`) stays separate from cost output (`metrics`)
- benchmark scripts can parse one stable JSON object
- grouped and ungrouped queries share the same reporting shape

## Metric Definitions

### `total_chunks`

Number of chunks in the table manifest.

### `skipped_chunks`

Chunks rejected before execution.

### `scanned_chunks`

Chunks whose column files were actually opened during execution.

### `rows_available`

Total rows in the manifest across all chunks.

### `rows_scanned`

Rows inside scanned chunks. This reflects how much row-level work the executor actually performed.

### `rows_matched`

Rows that passed exact row-level filtering and contributed to aggregation.

### `columns_read`

The required columns opened for execution, in schema order.

### `column_count`

The count of `columns_read`.

### `pruning_rate`

Defined as:

```text
skipped_chunks / total_chunks
```

## Example

For a query like:

```bash
tickdb query \
  --table bars \
  --agg sum:volume \
  --filter 'close>100'
```

the result now looks like:

```json
{
  "table": "bars",
  "rows": [
    {
      "sum_volume": 28369550
    }
  ],
  "metrics": {
    "total_chunks": 4,
    "skipped_chunks": 1,
    "scanned_chunks": 3,
    "rows_available": 20,
    "rows_scanned": 15,
    "rows_matched": 11,
    "columns_read": ["close", "volume"],
    "column_count": 2,
    "pruning_rate": 0.25
  },
  "result_row_count": 1
}
```

This is benchmark-friendly because the query result and the cost model now come back from one command.

## Implementation Boundary

Milestone 9 does not change the basic planning/execution split:

- Milestone 7 still selects candidate chunks from metadata
- Milestone 8 still reads those chunks and computes the final answer
- Milestone 9 reports how much work was avoided and how much work was done

The metrics are collected inside the executor, because that is where exact scan work is known.

## Tests

Milestone 9 extends query execution tests to verify:

- no-filter metrics
- pruned-query metrics
- grouped-query metrics
- CLI JSON output including the nested `metrics` object

That keeps the metrics shape stable for later benchmark scripts.
