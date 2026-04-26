# Layout Baseline Benchmark

Generated: 2026-04-25T15:13:24.460605+00:00

## Configuration

- rows: `100000`
- chunk_size: `10000`
- symbols: `AAPL, MSFT, NVDA, AMZN, GOOG, META, TSLA, AMD, AVGO, SPY`
- warmup_runs: `1`
- measured_runs: `3`

## full_scan_count

Full table count baseline with no pruning.

Business use case: Baseline scan cost for table-wide health checks and sanity counts.

| Layout | Median ms | Scanned Chunks | Rows Scanned | Pruning Rate |
| --- | ---: | ---: | ---: | ---: |
| `time` | `48.193` | `10` | `100000` | `0.0000` |
| `symbol_time` | `48.258` | `10` | `100000` | `0.0000` |

## time_window_avg_close

Average close over a narrow mid-stream time window.

Business use case: Measure market state over a recent window without targeting a symbol.

| Layout | Median ms | Scanned Chunks | Rows Scanned | Pruning Rate |
| --- | ---: | ---: | ---: | ---: |
| `time` | `7.0` | `1` | `10000` | `0.9000` |
| `symbol_time` | `65.462` | `10` | `100000` | `0.0000` |

## symbol_volume_sum

Volume sum for a single symbol across the full dataset.

Business use case: Evaluate single-name liquidity without constraining the time range.

| Layout | Median ms | Scanned Chunks | Rows Scanned | Pruning Rate |
| --- | ---: | ---: | ---: | ---: |
| `time` | `53.502` | `10` | `100000` | `0.0000` |
| `symbol_time` | `8.004` | `1` | `10000` | `0.9000` |

## symbol_time_avg_close

Average close for one symbol inside a wider time window.

Business use case: Typical market-data query for one name during a targeted period.

| Layout | Median ms | Scanned Chunks | Rows Scanned | Pruning Rate |
| --- | ---: | ---: | ---: | ---: |
| `time` | `6.631` | `1` | `10000` | `0.9000` |
| `symbol_time` | `8.931` | `1` | `10000` | `0.9000` |

