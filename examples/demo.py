"""Run a small end-to-end TickDB demo across both physical layouts."""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tickdb.data.generator import generate_csv
from tickdb.query.executor import execute_query
from tickdb.query.parser import build_query_spec
from tickdb.storage.compact import compact_table
from tickdb.storage.wal import ingest_csv_to_wal

ARTIFACTS_ROOT = Path(".demo_artifacts")
DATASET_PATH = ARTIFACTS_ROOT / "demo_ohlcv.csv"
TIME_ROOT = ARTIFACTS_ROOT / "time_root"
SYMBOL_TIME_ROOT = ARTIFACTS_ROOT / "symbol_time_root"


def main() -> int:
    _reset_artifacts()
    rows_written = generate_csv(
        output_path=DATASET_PATH,
        symbols=["AAPL", "MSFT", "NVDA", "AMZN", "GOOG"],
        rows=100_000,
        start_timestamp=1_704_067_200,
        step_seconds=60,
        seed=7,
    )
    print(f"generated {rows_written} rows at {DATASET_PATH}")

    for layout, root in [("time", TIME_ROOT), ("symbol_time", SYMBOL_TIME_ROOT)]:
        wal_rows, wal_path = ingest_csv_to_wal(
            root=root,
            table="bars",
            csv_path=DATASET_PATH,
        )
        print(f"[{layout}] ingested {wal_rows} rows into {wal_path}")

        compaction = compact_table(
            root=root,
            table="bars",
            chunk_size=10_000,
            layout=layout,
            block_size_rows=1_024,
        )
        print(
            f"[{layout}] compacted {compaction.rows_compacted} rows into "
            f"{compaction.chunk_count} chunks"
        )

    print("")
    print("Representative query:")
    print("sum(volume) where symbol = NVDA and close > 150")
    print("")

    query_spec = build_query_spec(
        table="bars",
        aggregation_tokens=["sum:volume"],
        filter_tokens=["symbol=NVDA", "close>150"],
        group_by_tokens=[],
    )

    time_result = execute_query(root=TIME_ROOT, query_spec=query_spec)
    symbol_time_result = execute_query(root=SYMBOL_TIME_ROOT, query_spec=query_spec)

    print("time layout result")
    print(json.dumps(time_result.to_dict(), indent=2))
    print("")
    print("symbol_time layout result")
    print(json.dumps(symbol_time_result.to_dict(), indent=2))
    print("")

    _print_summary(time_result.to_dict(), symbol_time_result.to_dict())
    return 0


def _reset_artifacts() -> None:
    if ARTIFACTS_ROOT.exists():
        shutil.rmtree(ARTIFACTS_ROOT)
    ARTIFACTS_ROOT.mkdir(parents=True, exist_ok=True)


def _print_summary(time_result: dict, symbol_time_result: dict) -> None:
    time_metrics = time_result["metrics"]
    symbol_time_metrics = symbol_time_result["metrics"]
    print("comparison summary")
    print(
        "time layout:"
        f" rows_scanned={time_metrics['rows_scanned']},"
        f" pruning_rate={time_metrics['pruning_rate']:.4f},"
        f" block_pruning_rate={time_metrics['block_pruning_rate']:.4f},"
        f" native_filter_used={time_metrics['native_filter_used']}"
    )
    print(
        "symbol_time layout:"
        f" rows_scanned={symbol_time_metrics['rows_scanned']},"
        f" pruning_rate={symbol_time_metrics['pruning_rate']:.4f},"
        f" block_pruning_rate={symbol_time_metrics['block_pruning_rate']:.4f},"
        f" native_filter_used={symbol_time_metrics['native_filter_used']}"
    )


if __name__ == "__main__":
    raise SystemExit(main())
