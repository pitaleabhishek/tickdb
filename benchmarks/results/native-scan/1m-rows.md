# Native Scan Comparison Benchmark

Generated: 2026-04-26T14:40:03.366656+00:00

## Configuration

- rows: `1000000`
- chunk_size: `10000`
- block_size_rows: `1024`
- symbols: `AAPL, MSFT, NVDA, AMZN, GOOG, META, TSLA, AMD, AVGO, SPY`
- warmup_runs: `1`
- measured_runs: `3`

## Narrow Time-Window Average Close

`SELECT AVG(close) FROM OHLCV_table WHERE timestamp BETWEEN t1 AND t2`

Average close over a narrow timestamp window to exercise the native int64 timestamp predicate path.

Business use case: Measure a short market interval where pruning already narrowed the scan and the remaining timestamp comparisons are the hot loop.

| Layout | Scan Mode | Median ms | Rows Scanned | Native Rows Evaluated | Speedup vs Python |
| --- | --- | ---: | ---: | ---: | --- |
| `time` | `python` | `2.442` | `1024` | `0` | `baseline` |
| `time` | `native` | `2.255` | `1024` | `1024` | `7.66% faster` |
| `symbol_time` | `python` | `8.76` | `10240` | `0` | `baseline` |
| `symbol_time` | `native` | `4.778` | `10240` | `10240` | `45.46% faster` |

- `time` python -> native: median_ms `2.442` -> `2.255` (7.66% faster)
- `symbol_time` python -> native: median_ms `8.76` -> `4.778` (45.46% faster)

## Single-Symbol Threshold Volume Sum

`SELECT SUM(volume) FROM OHLCV_table WHERE symbol = 'NVDA' AND close > 150`

Volume sum for NVDA above a close threshold to exercise the native float64 price predicate path.

Business use case: Typical single-name threshold query where symbol filtering stays in Python but the price predicate is a good native pushdown candidate.

| Layout | Scan Mode | Median ms | Rows Scanned | Native Rows Evaluated | Speedup vs Python |
| --- | --- | ---: | ---: | ---: | --- |
| `time` | `python` | `597.195` | `1000000` | `0` | `baseline` |
| `time` | `native` | `478.409` | `1000000` | `1000000` | `19.89% faster` |
| `symbol_time` | `python` | `25.808` | `28192` | `0` | `baseline` |
| `symbol_time` | `native` | `19.027` | `28192` | `28192` | `26.27% faster` |

- `time` python -> native: median_ms `597.195` -> `478.409` (19.89% faster)
- `symbol_time` python -> native: median_ms `25.808` -> `19.027` (26.27% faster)

