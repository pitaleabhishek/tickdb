# Milestone 11: Native Scan Kernel

## Summary

Milestone 11 adds a small C-based scan kernel for numeric predicates over surviving block-local column slices.

TickDB still keeps almost all system logic in Python:

- WAL ingestion
- compaction
- encodings
- chunk metadata
- block metadata
- query planning
- aggregation

The native code is intentionally narrow. It only replaces the hottest remaining loop in query execution: repeated numeric comparisons over surviving rows inside a block.

## Motivation

Before this milestone, TickDB already reduced the search space with:

1. chunk pruning
2. block pruning

But after that, Python still had to:

- iterate row-by-row
- build row dictionaries
- evaluate numeric filters one row at a time

That is the one place where native code is justified. The loop is simple, repeated, and measurable.

## What The Native Code Does

The C code walks a numeric array and writes a byte mask.

Example:

```text
close values: [10.5, 10.8, 20.5, 21.5]
filter:       close > 15
mask:         [0, 0, 1, 1]
```

Meaning:

- `0` = this row does not match
- `1` = this row matches

Python then continues only for rows whose mask byte is `1`.

## Scope

The native kernel currently supports:

- `>`
- `>=`
- `<`
- `<=`
- `between`

for:

- `float64` columns: `open/high/low/close`
- `int64` columns: `timestamp` and `volume`

The executor pushes down at most one eligible numeric filter into the native path for the first version. All filters are still rechecked in Python before aggregation, so correctness remains simple and explicit.

## Integration Shape

The query path is now:

1. prune chunks from `metadata/chunks.json`
2. prune blocks from `block_index.json`
3. read required block-local column slices
4. if one numeric filter is eligible, call the native scan kernel
5. use the returned mask to skip non-matching rows
6. recheck full filter truth in Python
7. aggregate in Python

This keeps the native boundary narrow while still making the hot loop faster.

## Fallback Behavior

The native path is optional.

TickDB tries to:

- compile the shared library lazily with `cc`
- load it through `ctypes`

If that fails, query execution automatically falls back to the pure-Python row filter path.

There is also an explicit CLI escape hatch:

```bash
tickdb query ... --disable-native-scan
```

That is useful for debugging and later Python-vs-native benchmarks.

## Metrics

Query output now includes:

- `native_filter_used`
- `native_rows_evaluated`

alongside the existing chunk/block pruning metrics.

This makes it easy to tell:

- whether the native path actually ran
- how many rows it evaluated

without changing query correctness or result shape.

## Why This Matters

This milestone does not change TickDB into a vectorized engine or a full native database runtime.

It does something narrower and more honest:

- keep the system understandable in Python
- move one real hot loop into C
- make the result benchmarkable

That is the right tradeoff for the project.
