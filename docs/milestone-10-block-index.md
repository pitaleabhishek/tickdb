# Milestone 10: Intra-Chunk Block Index

## Summary

Milestone 10 adds a finer-grained pruning layer inside compacted chunks.

Before this milestone, TickDB could only decide:

- skip this chunk
- or scan this entire chunk

After this milestone, TickDB can decide:

- skip this chunk
- scan this chunk, but only touch the blocks inside it that can still match

This is inspired by Parquet-style page indexes and BRIN-style block summaries, but implemented in TickDB's own storage format and tuned for OHLCV workloads.

## Motivation

Chunk-level metadata is useful, but still coarse.

Example:

- a chunk survives because `close_max > 100`
- but only a small region inside the chunk actually contains rows where `close > 100`

Without block-level metadata, TickDB still scans the whole chunk.

With a block index, TickDB can:

1. keep the chunk
2. skip the non-matching blocks inside it
3. only read required columns for the surviving block ranges

That reduces:

- rows scanned
- logical work per query
- scan cost for selective predicates

## On-Disk Format

Each chunk now includes:

```text
chunks/
  000000/
    meta.json
    block_index.json
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

`block_index.json` stores a list of fixed-size row blocks with pruning metadata.

Example shape:

```json
{
  "layout": "symbol_time",
  "block_size_rows": 1024,
  "block_count": 3,
  "blocks": [
    {
      "block_id": 0,
      "row_start": 0,
      "row_count": 1024,
      "symbols": ["AAPL"],
      "timestamp_min": 1704067200,
      "timestamp_max": 1704069900,
      "open_min": 99.2,
      "open_max": 101.4,
      "high_min": 99.6,
      "high_max": 102.0,
      "low_min": 98.9,
      "low_max": 100.7,
      "close_min": 99.1,
      "close_max": 101.8,
      "volume_min": 120000,
      "volume_max": 890000
    }
  ]
}
```

## Compaction Changes

`tickdb compact` now accepts:

```bash
tickdb compact --table bars --chunk-size 10000 --layout symbol_time --block-size-rows 1024
```

Compaction still writes:

- encoded columns
- `meta.json`
- `metadata/chunks.json`

and now also writes:

- `block_index.json`

The block index is built from the same row order already chosen by the table layout, so:

- `time` layout gives time-local blocks
- `symbol_time` layout gives symbol-local blocks

That makes block pruning a direct extension of the current physical-layout story.

## Execution Changes

The query path is now hierarchical:

1. planner loads `metadata/chunks.json`
2. planner selects candidate chunks
3. executor loads `block_index.json` for each surviving chunk
4. executor prunes non-matching blocks
5. executor reads only required columns for surviving block row ranges
6. executor applies exact row filters
7. executor aggregates the final result

Important detail:

- block metadata is still lossy
- row-level filters still recheck exact values before aggregation

So correctness stays the same. The feature only reduces unnecessary work.

## Metrics

Query output now includes block-level metrics alongside the existing chunk-level metrics:

- `total_blocks`
- `skipped_blocks`
- `scanned_blocks`
- `block_pruning_rate`

This gives later benchmark code a clean way to compare:

- chunk-only pruning
- chunk + block pruning
- different physical layouts

## Compatibility

The executor keeps backward compatibility with older compacted data that does not yet contain `block_index.json`.

If a chunk has no block index, TickDB synthesizes one logical block covering the whole chunk. That means:

- old demo data still queries correctly
- new compacted data gets the finer-grained pruning behavior

## Why This Matters

This milestone deepens the core TickDB thesis rather than widening the surface area.

TickDB is not just:

- WAL ingestion
- chunked columnar storage
- chunk pruning

It now also demonstrates:

- hierarchical pruning
- workload-aware intra-chunk skipping
- a more realistic analytical scan path for OHLCV queries

That makes the storage engine more compelling as a systems project and gives benchmarking a much stronger story than simply adding more syntax or more CLI features.
