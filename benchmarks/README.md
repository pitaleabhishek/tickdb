# Benchmarks

This directory contains the benchmark harnesses and saved result artifacts used to evaluate TickDB’s storage and execution choices.

## Goal

Measure how physical layout changes query cost while keeping:

- the logical dataset fixed
- the query fixed
- the chunk size fixed

The layout baseline harness materializes the same generated OHLCV data into two separate compacted layouts:

- `time`
- `symbol_time`

It then runs the same benchmark query set against both layouts and records:

- runtime
- scanned chunks
- rows scanned
- pruning rate
- columns read

The block-index harness keeps the logical dataset, query, and chunk size fixed, then compares:

- coarse blocks where `block_size_rows = chunk_size`
- fine blocks where `block_size_rows = 1024`

This isolates the impact of intra-chunk pruning on top of the existing chunk-level pruning logic.

## Query Set

The baseline harness currently runs four fixed cases:

1. `full_scan_count`
2. `time_window_avg_close`
3. `symbol_volume_sum`
4. `symbol_time_avg_close`

These are chosen to highlight:

- no-pruning baseline behavior
- time-favoring queries
- symbol-favoring queries
- symbol-plus-time workload behavior

The block-index harness currently runs two focused cases:

1. `narrow_time_window_avg_close`
2. `narrow_symbol_time_avg_close`

These are chosen to show where chunk pruning is not enough and finer-grained block summaries reduce the remaining scan work.

The native-scan harness currently runs two focused cases:

1. `narrow_time_window_avg_close`
2. `symbol_close_threshold_sum_volume`

These are chosen to exercise both native predicate families:

- `timestamp` range filtering through the `int64` native path
- `close` threshold filtering through the `float64` native path

## Running

Default run:

```bash
python3 benchmarks/run_layout_baselines.py
```

Block-index comparison run:

```bash
python3 benchmarks/run_block_index_comparison.py
```

Native-scan comparison run:

```bash
python3 benchmarks/run_native_scan_comparison.py
```

Smaller local run:

```bash
python3 benchmarks/run_layout_baselines.py --rows 100000 --force-rebuild
```

## Outputs

Artifacts are split into two categories:

- generated data and compacted roots under `benchmarks/.artifacts/`
- saved benchmark reports under `benchmarks/results/`

The harness writes:

- `benchmarks/results/layout_baseline_<rows>_rows.json`
- `benchmarks/results/layout_baseline_<rows>_rows.md`
- `benchmarks/results/block_index_comparison_<rows>_rows.json`
- `benchmarks/results/block_index_comparison_<rows>_rows.md`
- `benchmarks/results/native_scan_comparison_<rows>_rows.json`
- `benchmarks/results/native_scan_comparison_<rows>_rows.md`

The JSON file is the machine-readable artifact with the fuller metric payload. The Markdown file is the repo-friendly summary table.

## Current Baseline

The current committed baseline was run at:

- `1,000,000` rows
- `10,000` chunk size
- `10` symbols

Saved outputs:

- [`layout_baseline_1000000_rows.json`](results/layout_baseline_1000000_rows.json)
- [`layout_baseline_1000000_rows.md`](results/layout_baseline_1000000_rows.md)

Headline takeaways from that run:

- full-table scans are effectively neutral across layouts
- narrow time-window queries strongly favor `time`
- symbol-only queries strongly favor `symbol_time`
- symbol-plus-time queries still favor `symbol_time` when symbol locality dominates

## Block Index Comparison

The block-index comparison uses:

- the same dataset generation approach
- the same `time` and `symbol_time` layouts
- coarse blocks as the chunk-only baseline
- fine blocks as the new intra-chunk pruning path

Its job is to answer a different question:

> once the right chunks are selected, does a finer-grained intra-chunk index reduce the remaining scan cost?

## Native Scan Comparison

The native-scan comparison keeps:

- the same dataset
- the same compacted layout
- the same block size
- the same query

and changes only:

- `use_native_scan = False`
- vs `use_native_scan = True`

Its job is to answer:

> once the right rows are still going to be scanned, does moving the numeric predicate loop into C reduce runtime?

For presentation, the Markdown summaries keep only the most useful benchmark-facing columns. Metrics like `rows_matched` still remain in the raw JSON artifacts when deeper inspection is useful.
