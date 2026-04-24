# Milestone 7: Query Planning

This document explains the Milestone 7 implementation: how TickDB accepts a query-planning command, how it parses filters and aggregations, how it calculates required columns, and how it selects candidate chunks from metadata without executing the query.

## What Milestone 7 Adds

Before this milestone, TickDB had:

- a write path
- a compaction path
- a storage format
- mmap-based fixed-width readers

What it did not yet have was a query-facing layer that could turn user intent into a structured execution plan.

After this milestone, TickDB can:

- accept a planning-only query CLI command
- parse filters and aggregations into explicit dataclasses
- validate grouping rules
- calculate required columns
- load the table chunk manifest
- select candidate chunks using metadata
- print a deterministic JSON query plan

This milestone does not execute the query. It only plans it.

## Command Surface

Milestone 7 adds:

```bash
tickdb query-plan \
  --table bars \
  --agg avg:close \
  --filter symbol=AAPL \
  --filter close>100
```

Supported flags:

- `--table`: required
- `--root`: defaults to `.tickdb`
- `--agg`: repeatable, required
- `--filter`: repeatable, optional
- `--group-by`: repeatable, optional

Examples:

```bash
tickdb query-plan --table bars --agg count
tickdb query-plan --table bars --agg avg:close --filter symbol=AAPL
tickdb query-plan --table bars --agg sum:volume --filter timestamp>=1704067200
tickdb query-plan --table bars --agg count --group-by symbol
```

The command prints JSON rather than executing the query.

## Why Query Planning Is Separate

The point of this milestone is to keep planning separate from execution.

Planning answers:

- what filters did the user request?
- what aggregations did they request?
- which columns are actually needed?
- which chunks could possibly match from metadata alone?

Execution will answer later:

- which rows match?
- what values need to be aggregated?
- what is the final result?

That separation keeps the codebase modular and makes testing easier.

## Code Structure

This milestone adds three query modules.

### `tickdb/query/models.py`

Defines:

- schema-order constants
- `FilterSpec`
- `AggregationSpec`
- `QuerySpec`
- `ChunkCandidate`
- `QueryPlan`

These dataclasses are the stable internal representation for the planning layer.

### `tickdb/query/parser.py`

Responsibilities:

- parse filter tokens
- parse aggregation tokens
- validate grouping
- build a `QuerySpec`

This module is intentionally strict. It rejects unsupported syntax early rather than trying to guess what the user meant.

### `tickdb/query/planner.py`

Responsibilities:

- load `metadata/chunks.json`
- calculate required columns
- evaluate chunk metadata against query filters
- build a `QueryPlan`

This module does not read row data. It only works from metadata.

## Filter Syntax

Supported operators:

- `=`
- `>`
- `>=`
- `<`
- `<=`

Examples:

- `symbol=AAPL`
- `timestamp>=1704067200`
- `close>100`
- `volume<=5000000`

Typing rules:

- `symbol` => string
- `timestamp` => int
- `volume` => int
- `open/high/low/close` => float

Restriction:

- `symbol` currently supports only `=`

This keeps metadata pruning logic explicit and predictable.

## Aggregation Syntax

Supported aggregation tokens:

- `count`
- `count:*`
- `sum:<column>`
- `avg:<column>`
- `min:<column>`
- `max:<column>`

Examples:

- `count`
- `avg:close`
- `sum:volume`

Restrictions:

- `sum/avg/min/max` require numeric columns
- `count` does not require a value column

## Grouping

Initial support is intentionally narrow:

- `symbol` only

That matches the current project scope and avoids pretending to support arbitrary group-by logic before execution exists.

## Required Column Calculation

The planner computes required columns as the union of:

- all filter columns
- all aggregation columns that are not `None`
- all group-by columns

Important detail:

required columns are returned in schema order, not insertion order. That makes plans deterministic and easier to test.

Examples:

### Example 1

Query:

```text
avg(close) where symbol=AAPL
```

Required columns:

```text
symbol, close
```

### Example 2

Query:

```text
count group by symbol
```

Required columns:

```text
symbol
```

### Example 3

Query:

```text
sum(volume) where timestamp>=1704067200
```

Required columns:

```text
timestamp, volume
```

## Candidate Chunk Selection

The planner loads:

```text
.tickdb/tables/<table>/metadata/chunks.json
```

and builds `ChunkCandidate` objects from the manifest entries.

Then it applies metadata-level checks:

- `symbol=AAPL`
  - chunk must include `AAPL` in its `symbols`
- `timestamp>=T`
  - chunk must satisfy `timestamp_max >= T`
- `timestamp<=T`
  - chunk must satisfy `timestamp_min <= T`
- `close>100`
  - chunk must satisfy `close_max > 100`
- `close<=100`
  - chunk must satisfy `close_min <= 100`
- `close=100`
  - chunk must satisfy `close_min <= 100 <= close_max`

This is planning-level pruning. It reduces the candidate set but does not yet evaluate individual rows.

## Example Output

Example command:

```bash
tickdb query-plan \
  --table bars \
  --root .tickdb_demo \
  --agg avg:close \
  --filter symbol=AAPL
```

Example output shape:

```json
{
  "table": "bars",
  "filters": [
    {
      "column": "symbol",
      "operator": "=",
      "value": "AAPL"
    }
  ],
  "aggregations": [
    {
      "function": "avg",
      "column": "close"
    }
  ],
  "group_by": [],
  "required_columns": ["symbol", "close"],
  "candidate_chunks": [
    {
      "chunk_id": "000000",
      "path": "chunks/000000",
      "row_count": 5,
      "symbols": ["AAPL"]
    }
  ],
  "manifest_path": ".tickdb_demo/tables/bars/metadata/chunks.json",
  "total_chunks": 4,
  "selected_chunk_count": 1
}
```

That is the handoff object for later execution work.

## Testing Coverage

Milestone 7 adds tests for:

- parsing typed filter expressions
- parsing aggregation tokens
- required-column calculation
- candidate chunk selection from metadata
- group-by validation
- CLI JSON output

The tests are integration-oriented:

- they build a real compacted table
- then they run the planner against the real chunk manifest

So the tests verify actual storage metadata compatibility, not just isolated parser helpers.

## What This Milestone Does Not Yet Do

Milestone 7 does not:

- open row data files for execution
- apply row-level filters
- scan symbol ids or numeric columns during queries
- aggregate values
- produce final query answers

Those are the next milestone.

## Why This Matters

Milestone 4 made the read-side storage format real.

Milestone 6 made fixed-width columns readable through `mmap`.

Milestone 7 makes queries structurally understandable:

- parse user intent
- determine needed columns
- narrow the candidate chunk set
- produce a plan object

That gives Milestone 8 a clean contract: execute a `QueryPlan` rather than redoing parsing, validation, and metadata reasoning inside the executor.
