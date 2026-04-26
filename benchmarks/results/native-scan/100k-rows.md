# Native Scan Comparison Benchmark

Generated: 2026-04-26T14:39:37.338072+00:00

## Configuration

- rows: `100000`
- chunk_size: `10000`
- block_size_rows: `1024`
- symbols: `AAPL, MSFT, NVDA, AMZN, GOOG, META, TSLA, AMD, AVGO, SPY`
- warmup_runs: `1`
- measured_runs: `2`

## Narrow Time-Window Average Close

`SELECT AVG(close) FROM OHLCV_table WHERE timestamp BETWEEN t1 AND t2`

Average close over a narrow timestamp window to exercise the native int64 timestamp predicate path.

Business use case: Measure a short market interval where pruning already narrowed the scan and the remaining timestamp comparisons are the hot loop.

| Layout | Scan Mode | Median ms | Rows Scanned | Native Rows Evaluated | Speedup vs Python |
| --- | --- | ---: | ---: | ---: | --- |
| `time` | `python` | `10.555` | `1024` | `0` | `baseline` |
| `time` | `native` | `2.658` | `1024` | `1024` | `74.82% faster` |
| `symbol_time` | `python` | `21.474` | `10240` | `0` | `baseline` |
| `symbol_time` | `native` | `7.287` | `10240` | `10240` | `66.07% faster` |

- `time` python -> native: median_ms `10.555` -> `2.658` (74.82% faster)
- `symbol_time` python -> native: median_ms `21.474` -> `7.287` (66.07% faster)

## Single-Symbol Threshold Volume Sum

`SELECT SUM(volume) FROM OHLCV_table WHERE symbol = 'NVDA' AND close > 150`

Volume sum for NVDA above a close threshold to exercise the native float64 price predicate path.

Business use case: Typical single-name threshold query where symbol filtering stays in Python but the price predicate is a good native pushdown candidate.

| Layout | Scan Mode | Median ms | Rows Scanned | Native Rows Evaluated | Speedup vs Python |
| --- | --- | ---: | ---: | ---: | --- |
| `time` | `python` | `152.298` | `100000` | `0` | `baseline` |
| `time` | `native` | `130.424` | `100000` | `100000` | `14.36% faster` |
| `symbol_time` | `python` | `16.047` | `8192` | `0` | `baseline` |
| `symbol_time` | `native` | `12.701` | `8192` | `8192` | `20.85% faster` |

- `time` python -> native: median_ms `152.298` -> `130.424` (14.36% faster)
- `symbol_time` python -> native: median_ms `16.047` -> `12.701` (20.85% faster)

