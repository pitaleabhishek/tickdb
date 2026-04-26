# TickDB Design

This document explains the main design choices behind TickDB.

## Project Scope

TickDB is a small analytical database for OHLCV market data. It is intentionally narrow:

- fixed OHLCV schema
- local filesystem storage
- append to WAL, then compact into columnar chunks
- aggregation-heavy reads over compacted storage

The project is meant to make storage and execution tradeoffs explicit, not to be a general SQL database.

## Design Thesis

TickDB is built around one claim:

> physical layout plus lightweight metadata can materially reduce market-data query cost.

Everything else follows from that.

## Fixed Schema

TickDB is constrained to:

- `symbol: string`
- `timestamp: int64`
- `open: float64`
- `high: float64`
- `low: float64`
- `close: float64`
- `volume: int64`

That keeps the engine simple enough to go deep on storage, pruning, and benchmarkability.

## WAL First, Columnar Later

Incoming rows do not go straight into columnar storage.

Why:

- WAL append is simple and stable
- compaction wants sorted rows, chunk boundaries, and metadata
- doing columnar rewrite work on every append would complicate ingestion

So the write path is:

1. normalize rows
2. append to JSONL WAL
3. compact later into read-optimized storage

## Physical Layout Choices

TickDB exposes two physical layouts at compaction time:

- `time`
- `symbol_time`

The reason is not product flexibility. The reason is to make the layout tradeoff measurable.

### `time`

Rows are sorted by `(timestamp, symbol)`.

Best for:

- time-window queries across many symbols

### `symbol_time`

Rows are sorted by `(symbol, timestamp)`.

Best for:

- single-symbol scans
- symbol plus threshold predicates

## Encodings

TickDB uses simple encodings that preserve straightforward read logic.

### Dictionary Encoding

Used for:

- `symbol`

Example:

- dictionary: `["AAPL", "MSFT", "NVDA"]`
- ids: `[0, 0, 1, 2, 2]`

### Base + Offset Encoding

Used for:

- `timestamp`

Example:

- base: `1704067200`
- offsets: `[0, 60, 120, 180]`

### Fixed-Width Binary

Used for:

- `open`
- `high`
- `low`
- `close`
- `volume`

This keeps `mmap` reads and byte-offset math simple.

## Two-Stage Pruning

TickDB prunes at two granularities.

### Chunk-Level Pruning

Each chunk stores:

- symbol set
- min/max for every numeric column

This lets the planner skip whole chunks before reading heavy column data.

### Block-Level Pruning

Each surviving chunk is then split into fixed-size blocks, each with its own summaries.

This lets execution skip row ranges inside a chunk that already survived coarse pruning.

The pruning model is hierarchical:

1. chunk metadata decides whether a chunk can match at all
2. block metadata decides which row ranges inside that chunk can match
3. exact row filters still recheck values before aggregation

## mmap-Based Reads

Compacted numeric columns are fixed-width binary files. That means row `i` can be read by direct byte offset instead of row decoding through text formats.

This is why the engine can:

- read only required columns
- read row ranges inside a block
- hand raw block-local slices to the native scan path

## Native Scan Boundary

The native path is intentionally small.

What C does:

- evaluate one eligible numeric predicate over a block-local numeric array
- write a byte mask for matching rows

What Python still does:

- planning
- metadata pruning
- exact filter recheck
- aggregation
- metrics

This keeps the system understandable while still moving the hottest numeric loop out of Python.

## Metrics

Every executed query returns result rows plus execution metrics, including:

- scanned and skipped chunks
- scanned and skipped blocks
- rows scanned
- rows matched
- pruning rates
- native scan usage

Those metrics are part of the design, not an afterthought. They are what make the storage decisions benchmarkable.

## Deliberate Non-Goals

TickDB does not try to provide:

- a general SQL engine
- arbitrary schemas
- transactional semantics beyond append-style WAL ingest
- advanced query optimization
- vectorized batch execution
- multi-threaded scans
