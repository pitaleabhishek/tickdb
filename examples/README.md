# Examples

This directory contains one evaluator-facing example:

- `demo.py`: end-to-end demo across both physical layouts

## Recommended Entry Point

From the repo root:

```bash
make demo
```

That command runs `examples/demo.py`, which:

1. generates sample OHLCV data
2. ingests it into two separate roots
3. compacts one root as `time`
4. compacts one root as `symbol_time`
5. runs the same representative query against both
6. prints result rows plus pruning and native-scan metrics

If you want to run the script directly:

```bash
python3 examples/demo.py
```
