# Layout Baseline Benchmark

Generated: 2026-04-25T15:14:15.553983+00:00

## Configuration

- rows: `1000000`
- chunk_size: `10000`
- symbols: `AAPL, MSFT, NVDA, AMZN, GOOG, META, TSLA, AMD, AVGO, SPY`
- warmup_runs: `1`
- measured_runs: `3`

## Full Scan Count

`SELECT COUNT(*) FROM OHLCV_table`

Full table count baseline with no pruning.

Business use case: Baseline scan cost for table-wide health checks and sanity counts.

| Layout | Median ms | Scanned Chunks | Rows Scanned | Pruning Rate |
| --- | ---: | ---: | ---: | ---: |
| `time` | `486.034` | `100` | `1000000` | `0.0000` |
| `symbol_time` | `485.816` | `100` | `1000000` | `0.0000` |

## Narrow Time-Window Average Close

`SELECT AVG(close) FROM OHLCV_table WHERE timestamp BETWEEN t1 AND t2`

Average close over a narrow mid-stream time window.

Business use case: Measure market state over a recent window without targeting a symbol.

| Layout | Median ms | Scanned Chunks | Rows Scanned | Pruning Rate |
| --- | ---: | ---: | ---: | ---: |
| `time` | `11.711` | `1` | `10000` | `0.9900` |
| `symbol_time` | `68.414` | `10` | `100000` | `0.9000` |

## Single-Symbol Volume Sum

`SELECT SUM(volume) FROM OHLCV_table WHERE symbol = 'NVDA'`

Volume sum for a single symbol across the full dataset.

Business use case: Evaluate single-name liquidity without constraining the time range.

| Layout | Median ms | Scanned Chunks | Rows Scanned | Pruning Rate |
| --- | ---: | ---: | ---: | ---: |
| `time` | `557.618` | `100` | `1000000` | `0.0000` |
| `symbol_time` | `94.456` | `10` | `100000` | `0.9000` |

## Single-Symbol Windowed Average Close

`SELECT AVG(close) FROM OHLCV_table WHERE symbol = 'NVDA' AND timestamp BETWEEN t1 AND t2`

Average close for one symbol inside a wider time window.

Business use case: Typical market-data query for one name during a targeted period.

| Layout | Median ms | Scanned Chunks | Rows Scanned | Pruning Rate |
| --- | ---: | ---: | ---: | ---: |
| `time` | `34.312` | `5` | `50000` | `0.9500` |
| `symbol_time` | `11.623` | `1` | `10000` | `0.9900` |

