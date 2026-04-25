from __future__ import annotations

import csv
import json
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tickdb.query.executor import execute_query
from tickdb.query.parser import build_query_spec
from tickdb.storage.compact import compact_table
from tickdb.storage.wal import ingest_csv_to_wal

FIELDNAMES = ["symbol", "timestamp", "open", "high", "low", "close", "volume"]
REPO_ROOT = Path(__file__).resolve().parents[1]


class QueryExecutionTests(unittest.TestCase):
    def test_count_without_filters_uses_all_rows(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = self._prepare_table(Path(tmpdir))
            query_spec = build_query_spec(
                table="bars",
                aggregation_tokens=["count"],
                filter_tokens=[],
                group_by_tokens=[],
            )

            result = execute_query(root=root, query_spec=query_spec)

            self.assertEqual(result.rows, [{"count": 6}])
            self.assertEqual(
                result.metrics.to_dict(),
                {
                    "total_chunks": 3,
                    "skipped_chunks": 0,
                    "scanned_chunks": 3,
                    "total_blocks": 3,
                    "skipped_blocks": 0,
                    "scanned_blocks": 3,
                    "rows_available": 6,
                    "rows_scanned": 6,
                    "rows_matched": 6,
                    "columns_read": [],
                    "column_count": 0,
                    "pruning_rate": 0.0,
                    "block_pruning_rate": 0.0,
                },
            )

    def test_avg_close_for_symbol_returns_correct_value(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = self._prepare_table(Path(tmpdir))
            query_spec = build_query_spec(
                table="bars",
                aggregation_tokens=["avg:close"],
                filter_tokens=["symbol=AAPL"],
                group_by_tokens=[],
            )

            result = execute_query(root=root, query_spec=query_spec)

            self.assertEqual(result.rows, [{"avg_close": 11.0}])
            self.assertEqual(
                result.metrics.to_dict(),
                {
                    "total_chunks": 3,
                    "skipped_chunks": 2,
                    "scanned_chunks": 1,
                    "total_blocks": 1,
                    "skipped_blocks": 0,
                    "scanned_blocks": 1,
                    "rows_available": 6,
                    "rows_scanned": 2,
                    "rows_matched": 2,
                    "columns_read": ["symbol", "close"],
                    "column_count": 2,
                    "pruning_rate": 2 / 3,
                    "block_pruning_rate": 0.0,
                },
            )

    def test_numeric_filter_is_applied_at_row_level(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = self._prepare_table(Path(tmpdir))
            query_spec = build_query_spec(
                table="bars",
                aggregation_tokens=["sum:volume"],
                filter_tokens=["close>31"],
                group_by_tokens=[],
            )

            result = execute_query(root=root, query_spec=query_spec)

            self.assertEqual(result.rows, [{"sum_volume": 320}])
            self.assertEqual(
                result.metrics.to_dict(),
                {
                    "total_chunks": 3,
                    "skipped_chunks": 2,
                    "scanned_chunks": 1,
                    "total_blocks": 1,
                    "skipped_blocks": 0,
                    "scanned_blocks": 1,
                    "rows_available": 6,
                    "rows_scanned": 2,
                    "rows_matched": 1,
                    "columns_read": ["close", "volume"],
                    "column_count": 2,
                    "pruning_rate": 2 / 3,
                    "block_pruning_rate": 0.0,
                },
            )

    def test_group_by_symbol_returns_sorted_rows(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = self._prepare_table(Path(tmpdir))
            query_spec = build_query_spec(
                table="bars",
                aggregation_tokens=["count"],
                filter_tokens=[],
                group_by_tokens=["symbol"],
            )

            result = execute_query(root=root, query_spec=query_spec)

            self.assertEqual(
                result.rows,
                [
                    {"symbol": "AAPL", "count": 2},
                    {"symbol": "MSFT", "count": 2},
                    {"symbol": "NVDA", "count": 2},
                ],
            )
            self.assertEqual(
                result.metrics.to_dict(),
                {
                    "total_chunks": 3,
                    "skipped_chunks": 0,
                    "scanned_chunks": 3,
                    "total_blocks": 3,
                    "skipped_blocks": 0,
                    "scanned_blocks": 3,
                    "rows_available": 6,
                    "rows_scanned": 6,
                    "rows_matched": 6,
                    "columns_read": ["symbol"],
                    "column_count": 1,
                    "pruning_rate": 0.0,
                    "block_pruning_rate": 0.0,
                },
            )

    def test_cli_query_prints_json(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = self._prepare_table(Path(tmpdir))
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tickdb.cli",
                    "query",
                    "--table",
                    "bars",
                    "--root",
                    str(root),
                    "--agg",
                    "sum:volume",
                    "--filter",
                    "close>31",
                ],
                cwd=REPO_ROOT,
                check=True,
                capture_output=True,
                text=True,
            )

            payload = json.loads(result.stdout)
            self.assertEqual(payload["table"], "bars")
            self.assertEqual(payload["rows"], [{"sum_volume": 320}])
            self.assertEqual(
                payload["metrics"],
                {
                    "total_chunks": 3,
                    "skipped_chunks": 2,
                    "scanned_chunks": 1,
                    "total_blocks": 1,
                    "skipped_blocks": 0,
                    "scanned_blocks": 1,
                    "rows_available": 6,
                    "rows_scanned": 2,
                    "rows_matched": 1,
                    "columns_read": ["close", "volume"],
                    "column_count": 2,
                    "pruning_rate": 2 / 3,
                    "block_pruning_rate": 0.0,
                },
            )

    def test_block_index_prunes_blocks_inside_candidate_chunk(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = self._prepare_block_index_table(Path(tmpdir))
            query_spec = build_query_spec(
                table="bars",
                aggregation_tokens=["sum:volume"],
                filter_tokens=["symbol=AAPL", "close>40"],
                group_by_tokens=[],
            )

            result = execute_query(root=root, query_spec=query_spec)

            self.assertEqual(result.rows, [{"sum_volume": 1010}])
            self.assertEqual(
                result.metrics.to_dict(),
                {
                    "total_chunks": 1,
                    "skipped_chunks": 0,
                    "scanned_chunks": 1,
                    "total_blocks": 3,
                    "skipped_blocks": 2,
                    "scanned_blocks": 1,
                    "rows_available": 6,
                    "rows_scanned": 2,
                    "rows_matched": 2,
                    "columns_read": ["symbol", "close", "volume"],
                    "column_count": 3,
                    "pruning_rate": 0.0,
                    "block_pruning_rate": 2 / 3,
                },
            )

    def _prepare_table(self, tmp_path: Path) -> Path:
        rows = [
            self._row("AAPL", 1000, 10.0, 11.0, 9.5, 10.5, 100),
            self._row("AAPL", 1010, 10.5, 12.0, 10.0, 11.5, 120),
            self._row("MSFT", 1000, 20.0, 21.0, 19.5, 20.5, 200),
            self._row("MSFT", 1010, 20.5, 22.0, 20.0, 21.5, 220),
            self._row("NVDA", 1000, 30.0, 31.0, 29.5, 30.5, 300),
            self._row("NVDA", 1010, 30.5, 32.0, 30.0, 31.5, 320),
        ]

        root = tmp_path / ".tickdb"
        csv_path = tmp_path / "input.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)

        ingest_csv_to_wal(root=root, table="bars", csv_path=csv_path)
        compact_table(root=root, table="bars", chunk_size=2, layout="symbol_time")
        return root

    def _prepare_block_index_table(self, tmp_path: Path) -> Path:
        rows = [
            self._row("AAPL", 1000, 10.0, 11.0, 9.5, 10.5, 100),
            self._row("AAPL", 1010, 10.5, 12.0, 10.0, 11.5, 110),
            self._row("AAPL", 1020, 50.0, 51.0, 49.5, 50.5, 500),
            self._row("AAPL", 1030, 50.5, 52.0, 50.0, 51.5, 510),
            self._row("AAPL", 1040, 12.0, 13.0, 11.5, 12.5, 120),
            self._row("AAPL", 1050, 12.5, 14.0, 12.0, 13.5, 130),
        ]

        root = tmp_path / ".tickdb"
        csv_path = tmp_path / "input.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)

        ingest_csv_to_wal(root=root, table="bars", csv_path=csv_path)
        compact_table(
            root=root,
            table="bars",
            chunk_size=6,
            layout="symbol_time",
            block_size_rows=2,
        )
        return root

    @staticmethod
    def _row(
        symbol: str,
        timestamp: int,
        open_price: float,
        high_price: float,
        low_price: float,
        close_price: float,
        volume: int,
    ) -> dict[str, object]:
        return {
            "symbol": symbol,
            "timestamp": timestamp,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": volume,
        }
