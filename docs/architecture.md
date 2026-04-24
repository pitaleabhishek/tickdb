# TickDB Architecture

## End-to-End Flow

```mermaid
flowchart LR
    A[CSV or Synthetic OHLCV Data] --> B[Row Validation]
    B --> C[Append-Only WAL]
    C --> D[Compaction]
    D --> E[Chunked Column Files]
    E --> F[Chunk Metadata]
    F --> G[Query Planner]
    E --> G
    G --> H[Column Projection]
    H --> I[mmap-Based Reads]
    I --> J[Python or Native Scan]
    J --> K[Aggregation]
    K --> L[Result + Execution Metrics]
```

## Storage Layers

TickDB has two distinct storage layers.

### Row-Oriented Ingestion Layer

Purpose:

- append-friendly writes
- simple ingest boundary
- replay source for compaction

Format:

- per-table JSONL WAL

### Column-Oriented Read Layer

Purpose:

- read only required columns
- keep numeric data in fixed-width binary format
- prune chunks before reading heavy data

Format:

- per-chunk binary column files plus metadata

## Planned Chunk Layout

```text
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

Design implications:

- chunk-local dictionaries keep metadata and decode paths simple
- per-chunk files align naturally with pruning decisions
- fixed-width numeric columns are friendly to mmap reads

## Storage Walkthrough

One table directory contains two different storage layers:

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

How to read that layout:

- `wal/` is the write-side log. It stores full JSON rows and is easy to append to.
- `metadata/table.json` stores table-level schema metadata.
- `metadata/chunks.json` stores the table-level manifest for compacted chunks.
- `chunks/000000/` is one compacted slice of rows in columnar form.
- `meta.json` stores per-chunk statistics used for pruning.
- `symbol.dict.json` plus `symbol.ids.u32` store the encoded symbol column.
- `timestamp.base` plus `timestamp.offsets.i64` store the encoded timestamp column.
- `open/high/low/close/volume` are fixed-width binary column files.

The important idea is that the WAL is the ingestion layer, while `chunks/` is the analytical read layer.

## Query Path

```mermaid
flowchart TD
    A[CLI Query] --> B[Parse Into Query Object]
    B --> C[Build Query Plan]
    C --> D[Determine Required Columns]
    C --> E[Load Chunk Metadata]
    E --> F{Chunk Can Match?}
    F -- No --> G[Skip Chunk]
    F -- Yes --> H[Read Required Columns]
    D --> H
    H --> I[Decode Needed Values]
    I --> J[Apply Filters]
    J --> K[Aggregate]
    K --> L[Merge Chunk Results]
    L --> M[Print Result Table]
    L --> N[Print Scan Metrics]
```

## Pruning Rules

Chunks can be skipped before reading column data when:

- the query symbol is not in the chunk symbol set
- the query time range does not overlap chunk time bounds
- numeric predicates cannot be satisfied from min/max metadata

Example:

If a query asks for `symbol = NVDA` and `close > 500`, a chunk can be skipped if:

- `NVDA` is absent from `symbols`
- or `close_max <= 500`

## Layout Modes

TickDB will compare at least two physical layouts.

### Time Layout

Rows sorted by:

- `timestamp`

Expected benefit:

- good time pruning

Expected weakness:

- weaker symbol locality

### Symbol-Time Layout

Rows sorted by:

- `symbol`
- `timestamp`

Expected benefits:

- better symbol pruning
- stronger symbol locality
- more effective optional symbol RLE

This layout comparison is one of the core analytical stories in the project.

## Native Scan Boundary

The system boundary is:

- Python for CLI, storage orchestration, metadata, planning, and aggregation
- C for hot numeric filter loops

That division keeps the project understandable while still demonstrating low-level performance work where it matters.
