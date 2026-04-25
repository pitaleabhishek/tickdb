# TickDB

TickDB is a small analytical database for OHLCV market data, built from scratch in Python with a small C scan kernel for performance-critical numeric filters.

The project is intentionally narrow: it focuses on the internals behind analytical storage engines used for market-data workloads rather than on production features such as transactions, concurrency, or full SQL support.

The central idea is simple:

1. ingest rows into an append-only WAL
2. compact WAL rows into chunked columnar files
3. answer analytical queries by reading only required columns and pruning irrelevant chunks

The submission goal is not "build a full database." The goal is to demonstrate that physical layout, column projection, and market-aware metadata materially change query cost.

## Why This Project

The assignment asks for a non-trivial system built end-to-end. TickDB is a good fit because it exercises several layers at once:

- ingestion and durability boundaries
- storage layout design
- encoding decisions
- query planning and execution
- workload-aware pruning
- low-level performance work
- benchmarking and correctness testing

That makes the repository useful as both a working system and a clear engineering artifact.

## Status

Current implementation:

- Synthetic OHLCV data generation
- CSV-to-WAL ingestion
- WAL-to-columnar compaction into chunked storage
- Per-chunk metadata and table-level chunk manifest
- mmap-based readers for fixed-width numeric and timestamp-offset columns
- query planning over chunk metadata with explicit required-column calculation
- query execution over compacted chunks with `count/sum/avg/min/max`
- `group by symbol` execution
- structured pruning and scan metrics in query output
- Project packaging, CLI, and test scaffolding

Planned next:

- native scan kernel benchmarks
- benchmark scripts over layout and pruning modes

## Project Shape

TickDB uses a fixed OHLCV schema:

- `symbol`
- `timestamp`
- `open`
- `high`
- `low`
- `close`
- `volume`

The intended storage model is per-table, per-chunk columnar layout:

```text
.tickdb/
  tables/
    bars/
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

## Architecture

TickDB follows a split write/read design:

1. Incoming rows are appended to an immutable, row-oriented WAL.
2. WAL rows are compacted into chunked columnar files.
3. Queries read only the required columns, use chunk metadata for pruning, and aggregate over the remaining rows.

That design keeps ingestion simple while making analytical reads efficient.

Additional design notes live in:

- [docs/design.md](docs/design.md)
- [docs/architecture.md](docs/architecture.md)
- [docs/milestone-04-compaction.md](docs/milestone-04-compaction.md)
- [docs/milestone-06-mmap-readers.md](docs/milestone-06-mmap-readers.md)
- [docs/milestone-07-query-planning.md](docs/milestone-07-query-planning.md)
- [docs/milestone-08-query-execution.md](docs/milestone-08-query-execution.md)
- [docs/milestone-09-pruning-metrics.md](docs/milestone-09-pruning-metrics.md)

## Quickstart

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Generate synthetic OHLCV data:

```bash
tickdb generate \
  --symbols AAPL,MSFT,NVDA \
  --rows 10000 \
  --output data/sample_ohlcv.csv
```

Ingest a CSV file into the WAL for table `bars`:

```bash
tickdb ingest \
  --table bars \
  --file data/sample_ohlcv.csv
```

Compact the WAL into chunked columnar storage:

```bash
tickdb compact \
  --table bars \
  --chunk-size 10000 \
  --layout time
```

The resulting storage lives under:

```text
.tickdb/tables/bars/
```

Plan a query without executing it:

```bash
tickdb query-plan \
  --table bars \
  --agg avg:close \
  --filter symbol=AAPL
```

Execute a query against compacted storage:

```bash
tickdb query \
  --table bars \
  --agg avg:close \
  --filter symbol=AAPL
```

The query result includes both final rows and a nested `metrics` object for pruning and scan cost.

Run tests:

```bash
python3 -m unittest discover -s tests
```

## Success Criteria

The project is successful if it can:

- generate or ingest OHLCV data
- append rows into a per-table WAL
- compact WAL data into chunked columnar files
- read only required columns for a query
- prune chunks using symbol, time, and numeric metadata
- return correct analytical aggregates
- benchmark full scan vs pruned scan paths
- benchmark Python filtering vs native filtering
- run from a fresh clone with clear instructions

## Non-Goals

TickDB does not aim to support:

- Full SQL parsing
- Joins
- Distributed execution
- Concurrent writers
- Production-grade crash recovery
- Real-time streaming ingestion

## Planned Query Surface

TickDB will not implement full SQL. The CLI query layer will support a small analytical surface:

- filters
- `count`
- `sum`
- `avg`
- `min`
- `max`
- `group by symbol`
- execution metrics for scan cost and pruning rate

Representative query classes:

1. time-range aggregate
2. symbol + time-range aggregate
3. group-by-symbol over a time window

Useful commands:

```bash
make install
make test
make clean
```
