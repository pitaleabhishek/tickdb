# Demo

One-command end-to-end demo:

```bash
make demo
```

That command will:

- generate sample OHLCV data
- ingest it into two separate roots
- compact one root as `time`
- compact one root as `symbol_time`
- run the same representative query against both
- print the result rows plus execution metrics

Generate sample data:

```bash
tickdb generate --symbols AAPL,MSFT,NVDA --rows 10000 --output data/sample_ohlcv.csv
```

Ingest into the WAL:

```bash
tickdb ingest --table bars --file data/sample_ohlcv.csv
```

Compact the WAL into chunked columnar files:

```bash
tickdb compact --table bars --chunk-size 10000 --layout time
```
