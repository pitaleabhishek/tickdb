# Block Index Comparison Benchmark

Generated: 2026-04-25T19:40:00.732574+00:00

## Configuration

- rows: `1000000`
- chunk_size: `10000`
- fine_block_size_rows: `1024`
- symbols: `AAPL, MSFT, NVDA, AMZN, GOOG, META, TSLA, AMD, AVGO, SPY`
- warmup_runs: `1`
- measured_runs: `3`

Coarse mode sets `block_size_rows = chunk_size`, which makes block pruning equivalent to chunk-only scanning.

## narrow_time_window_avg_close

Average close over a narrow time window small enough to fit inside one chunk but much smaller than the chunk itself.

Business use case: Measure a short market interval where chunk pruning alone is too coarse and intra-chunk skipping should reduce scan work.

| Layout | Block Mode | Median ms | Scanned Chunks | Scanned Blocks | Rows Scanned | Block Pruning Rate |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `time` | `coarse` | `9.294` | `1` | `1` | `10000` | `0.0000` |
| `time` | `fine` | `2.04` | `1` | `1` | `1024` | `0.9000` |
| `symbol_time` | `coarse` | `70.509` | `10` | `10` | `100000` | `0.0000` |
| `symbol_time` | `fine` | `9.71` | `10` | `10` | `10240` | `0.9000` |

- `time` coarse -> fine: rows_scanned `10000` -> `1024` (89.76% reduction), median_ms `9.294` -> `2.04` (78.05% reduction)
- `symbol_time` coarse -> fine: rows_scanned `100000` -> `10240` (89.76% reduction), median_ms `70.509` -> `9.71` (86.23% reduction)

## narrow_symbol_time_avg_close

Average close for one symbol inside a very narrow time window.

Business use case: Typical single-name OHLCV query where chunk pruning narrows the candidate set and block pruning should reduce the remaining scan.

| Layout | Block Mode | Median ms | Scanned Chunks | Scanned Blocks | Rows Scanned | Block Pruning Rate |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `time` | `coarse` | `7.726` | `1` | `1` | `10000` | `0.0000` |
| `time` | `fine` | `1.855` | `1` | `1` | `1024` | `0.9000` |
| `symbol_time` | `coarse` | `10.247` | `1` | `1` | `10000` | `0.0000` |
| `symbol_time` | `fine` | `1.918` | `1` | `1` | `1024` | `0.9000` |

- `time` coarse -> fine: rows_scanned `10000` -> `1024` (89.76% reduction), median_ms `7.726` -> `1.855` (75.99% reduction)
- `symbol_time` coarse -> fine: rows_scanned `10000` -> `1024` (89.76% reduction), median_ms `10.247` -> `1.918` (81.28% reduction)

