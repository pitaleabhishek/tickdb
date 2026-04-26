# Benchmark Results Summary

This file is the single-screen view of the committed `1m-rows` benchmark results.

## 1. Layout Benchmark

| Query | `time` ms | `symbol_time` ms | Winner |
| --- | ---: | ---: | --- |
| `SELECT COUNT(*) FROM OHLCV_table` | `486.034` | `485.816` | neutral |
| `SELECT AVG(close) FROM OHLCV_table WHERE timestamp BETWEEN t1 AND t2` | `11.711` | `68.414` | `time` |
| `SELECT SUM(volume) FROM OHLCV_table WHERE symbol = 'NVDA'` | `557.618` | `94.456` | `symbol_time` |
| `SELECT AVG(close) FROM OHLCV_table WHERE symbol = 'NVDA' AND timestamp BETWEEN t1 AND t2` | `34.312` | `11.623` | `symbol_time` |

What this shows:

- full scans are effectively neutral across layouts
- `time` wins when the predicate is mostly about timestamp
- `symbol_time` wins when the predicate is mostly about symbol locality

## 2. Block Pruning Benchmark

| Query | Setup | Median ms | Rows Scanned |
| --- | --- | ---: | ---: |
| `SELECT AVG(close) FROM OHLCV_table WHERE timestamp BETWEEN t1 AND t2` | `time`, chunk-only | `9.294` | `10000` |
| `SELECT AVG(close) FROM OHLCV_table WHERE timestamp BETWEEN t1 AND t2` | `time`, chunk + block | `2.040` | `1024` |
| `SELECT AVG(close) FROM OHLCV_table WHERE symbol = 'NVDA' AND timestamp BETWEEN t1 AND t2` | `symbol_time`, chunk-only | `10.247` | `10000` |
| `SELECT AVG(close) FROM OHLCV_table WHERE symbol = 'NVDA' AND timestamp BETWEEN t1 AND t2` | `symbol_time`, chunk + block | `1.918` | `1024` |

What this shows:

- chunk pruning is not always enough once a chunk survives
- block metadata removes most of the remaining row scan inside that chunk

## 3. Native Scan Benchmark

| Query | Layout | Python ms | Native ms | Speedup |
| --- | --- | ---: | ---: | --- |
| `SELECT AVG(close) FROM OHLCV_table WHERE timestamp BETWEEN t1 AND t2` | `time` | `2.442` | `2.255` | `7.66% faster` |
| `SELECT AVG(close) FROM OHLCV_table WHERE timestamp BETWEEN t1 AND t2` | `symbol_time` | `8.760` | `4.778` | `45.46% faster` |
| `SELECT SUM(volume) FROM OHLCV_table WHERE symbol = 'NVDA' AND close > 150` | `time` | `597.195` | `478.409` | `19.89% faster` |
| `SELECT SUM(volume) FROM OHLCV_table WHERE symbol = 'NVDA' AND close > 150` | `symbol_time` | `25.808` | `19.027` | `26.27% faster` |

What this shows:

- pruning reduces the working set first
- native scan then reduces CPU cost on the rows that still must be examined

## Detailed Reports

- [`layout/1m-rows.md`](layout/1m-rows.md)
- [`block-pruning/1m-rows.md`](block-pruning/1m-rows.md)
- [`native-scan/1m-rows.md`](native-scan/1m-rows.md)
