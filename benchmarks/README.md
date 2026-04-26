# Benchmarks

This directory contains three benchmark harnesses and their saved outputs.

## Start Here

If you want the committed benchmark story on one screen, start with:

- [`results/README.md`](results/README.md)

## What Each Benchmark Measures

| Script | Question it answers |
| --- | --- |
| `run_layout_benchmarks.py` | Does `time` or `symbol_time` win for a given OHLCV query shape? |
| `run_block_pruning_benchmarks.py` | Once the right chunks are selected, does block-level pruning reduce the remaining scan work? |
| `run_native_scan_benchmarks.py` | Once pruning has reduced the working set, does moving the numeric predicate loop into C reduce runtime? |

## How To Run Them

```bash
python3 benchmarks/run_layout_benchmarks.py
python3 benchmarks/run_block_pruning_benchmarks.py
python3 benchmarks/run_native_scan_benchmarks.py
```

Smaller local run:

```bash
python3 benchmarks/run_layout_benchmarks.py --rows 100000 --force-rebuild
```

## Directory Layout

```text
benchmarks/
  README.md
  query_cases.py
  run_layout_benchmarks.py
  run_block_pruning_benchmarks.py
  run_native_scan_benchmarks.py
  results/
    layout/
      100k-rows.json
      100k-rows.md
      1m-rows.json
      1m-rows.md
    block-pruning/
      100k-rows.json
      100k-rows.md
      1m-rows.json
      1m-rows.md
    native-scan/
      100k-rows.json
      100k-rows.md
      1m-rows.json
      1m-rows.md
```

## How To Read The Results

Then drill down into the individual reports:

1. `results/layout/1m-rows.md`
2. `results/block-pruning/1m-rows.md`
3. `results/native-scan/1m-rows.md`

Interpretation order:

1. Layout changes how many chunks can be skipped.
2. Block pruning reduces work inside surviving chunks.
3. Native scan reduces CPU cost on the rows that still must be examined.

Each benchmark writes:

- a `.json` file with the full metric payload
- a `.md` file with the human-readable summary

The committed `1m-rows` reports are the ones referenced from the top-level README.
