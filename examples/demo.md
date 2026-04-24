# Demo

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
