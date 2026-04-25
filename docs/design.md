# TickDB Design

## Goal

TickDB is a small analytical database for OHLCV market data. The purpose of the project is to demonstrate the core mechanics behind market-data analytics engines:

- write-ahead logging
- columnar storage
- column-specific encodings
- metadata-driven chunk pruning
- memory-mapped reads
- native scan execution for numeric filters

The target is a serious systems prototype, not a production database.

## Design Thesis

TickDB is built around one claim:

> physical layout plus chunk metadata can materially reduce market-data query cost.

Everything in the project should support that claim with code, tests, and benchmarks.

## Fixed Schema

TickDB is intentionally schema-constrained around OHLCV bars:

- `symbol: string`
- `timestamp: int64`
- `open: float64`
- `high: float64`
- `low: float64`
- `close: float64`
- `volume: int64`

That constraint keeps the storage engine simple enough to finish cleanly while still being non-trivial.

## On-Disk Layout

TickDB stores data under a local `.tickdb` root.

```text
.tickdb/
  tables/
    <table>/
      wal/
        000001.jsonl
      metadata/
        table.json
        chunks.json
      chunks/
        000000/
          meta.json
          symbol.dict.json
          symbol.ids.u32
          timestamp.base
          timestamp.offsets.i64
          open.f64
          high.f64
          low.f64
          close.f64
          volume.i64
```

This layout matters:

- the WAL is row-oriented and append-friendly
- compacted chunks are column-oriented and scan-friendly
- chunk boundaries are explicit, which makes pruning and metrics straightforward

## Write Path

The write path is deliberately simple:

1. read OHLCV rows from CSV or the synthetic generator
2. validate and normalize row types
3. append rows to a per-table JSONL WAL

The WAL is not the final analytical storage format. It is the ingestion boundary.

## Compaction Strategy

Compaction reads WAL rows and writes chunked columnar storage.

Planned compaction steps:

1. load WAL rows
2. sort rows by chosen layout mode
3. split rows into fixed-size chunks
4. encode and write each column per chunk
5. write chunk metadata
6. update table-level manifest metadata

Initial layout modes:

- `time`
- `symbol_time`

The `symbol_time` mode is important because it should improve symbol pruning and make optional symbol RLE more effective.

## Encoding Strategy

TickDB uses simple, explicit encodings rather than clever compression.

### Symbol

Primary path:

- dictionary encoding

Optional path:

- dictionary encoding plus RLE over encoded symbol IDs

RLE is only worth the complexity when rows are symbol-clustered.

### Timestamp

Use:

- `base timestamp + int64 offsets`

This preserves a delta-style representation while keeping reads simple and random-access friendly.

### Numeric Columns

Use fixed-width binary:

- `open/high/low/close`: `float64`
- `volume`: `int64`

This makes mmap-based reads and offset calculations simple and predictable.

## Chunk Metadata

Each chunk will store metadata used for planning and pruning.

Expected fields:

- `row_count`
- `symbols`
- `timestamp_min`
- `timestamp_max`
- `open_min/open_max`
- `high_min/high_max`
- `low_min/low_max`
- `close_min/close_max`
- `volume_min/volume_max`

This is essentially a small set of zone maps plus a symbol set.

## Query Model

TickDB does not need a SQL parser. The query interface will be a small CLI surface that supports:

- projection
- filters
- aggregations
- `group by symbol`

The query interface uses explicit repeated flags rather than SQL text, for example:

```bash
tickdb query-plan \
  --table bars \
  --agg avg:close \
  --filter symbol=AAPL \
  --filter close>100
```

Representative filters:

- `symbol = AAPL`
- `timestamp between T1 and T2`
- `close > X`

Representative aggregations:

- `count`
- `sum`
- `avg`
- `min`
- `max`

The execution command uses the same grammar:

```bash
tickdb query \
  --table bars \
  --agg avg:close \
  --filter symbol=AAPL
```

## Query Execution Plan

Current execution flow:

1. parse CLI arguments into a structured query
2. identify required columns
3. load chunk metadata
4. prune impossible chunks
5. read only required columns for remaining chunks
6. apply filters
7. aggregate result rows
8. print deterministic JSON results plus execution metrics

Current execution scope:

- `count`
- `sum`
- `avg`
- `min`
- `max`
- `group by symbol`

Later metrics work should include:

- total chunks
- skipped chunks
- scanned chunks
- rows available
- rows scanned
- rows matched
- columns read
- pruning rate

## Native Scan Kernel

Python will orchestrate the system. C will handle the hottest numeric scan loops.

Initial native kernel scope:

- `>`
- `<`
- `between`

Preferred native output:

- a byte mask indicating which rows matched

That integrates more cleanly with aggregation than returning a list of row indexes.

## Benchmark Story

The final repo should benchmark exactly the claims the design makes.

Core comparisons:

1. full scan vs time-only pruning vs symbol-time pruning
2. projected columns read vs total columns
3. Python numeric filtering vs native numeric filtering

The benchmark goal is not absolute speed. The benchmark goal is demonstrating reduced work.

## Testing Strategy

Tests should cover:

- synthetic data generation
- WAL ingestion and replay
- encoding and decoding correctness
- chunk metadata correctness
- pruning correctness
- query aggregation correctness
- fallback behavior when native code is unavailable

## Non-Goals

TickDB intentionally excludes:

- full SQL parsing
- joins
- concurrent writers
- multi-threaded execution
- distributed storage
- production-grade recovery
- object storage
- JIT or SIMD work

## Documentation Strategy

The repo should tell a coherent story in commit history:

1. scaffold plus baseline docs
2. ingest and WAL
3. compaction and storage format
4. query planning and pruning
5. native scan path
6. benchmarks and final polish

That history is almost as important as the final code.
