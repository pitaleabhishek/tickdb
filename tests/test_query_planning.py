from __future__ import annotations

import csv
import json
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tickdb.query.parser import build_query_spec, parse_aggregation_token, parse_filter_token
from tickdb.query.planner import build_query_plan
from tickdb.storage.compact import compact_table
from tickdb.storage.wal import ingest_csv_to_wal

FIELDNAMES = ["symbol", "timestamp", "open", "high", "low", "close", "volume"]


class QueryPlanningTests(unittest.TestCase):
    def test_filter_parsing_casts_types(self) -> None:
        symbol_filter = parse_filter_token("symbol=AAPL")
        close_filter = parse_filter_token("close>100.5")
        timestamp_filter = parse_filter_token("timestamp>=1704067200")

        self.assertEqual(symbol_filter.column, "symbol")
        self.assertEqual(symbol_filter.operator, "=")
        self.assertEqual(symbol_filter.value, "AAPL")

        self.assertEqual(close_filter.column, "close")
        self.assertEqual(close_filter.operator, ">")
        self.assertEqual(close_filter.value, 100.5)

        self.assertEqual(timestamp_filter.column, "timestamp")
        self.assertEqual(timestamp_filter.operator, ">=")
        self.assertEqual(timestamp_filter.value, 1_704_067_200)

    def test_aggregation_parsing_supports_count_and_avg(self) -> None:
        count_agg = parse_aggregation_token("count")
        avg_agg = parse_aggregation_token("avg:close")

        self.assertEqual(count_agg.function, "count")
        self.assertIsNone(count_agg.column)
        self.assertEqual(avg_agg.function, "avg")
        self.assertEqual(avg_agg.column, "close")

    def test_required_columns_and_candidate_chunks_are_planned(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = self._prepare_table(Path(tmpdir))
            query_spec = build_query_spec(
                table="bars",
                aggregation_tokens=["avg:close"],
                filter_tokens=["symbol=AAPL"],
                group_by_tokens=[],
            )

            query_plan = build_query_plan(root=root, query_spec=query_spec)

            self.assertEqual(query_plan.required_columns, ["symbol", "close"])
            self.assertEqual(query_plan.total_chunks, 3)
            self.assertEqual(
                [chunk.chunk_id for chunk in query_plan.candidate_chunks],
                ["000000"],
            )

    def test_numeric_filter_prunes_candidate_chunks(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = self._prepare_table(Path(tmpdir))
            query_spec = build_query_spec(
                table="bars",
                aggregation_tokens=["sum:volume"],
                filter_tokens=["close>25"],
                group_by_tokens=[],
            )

            query_plan = build_query_plan(root=root, query_spec=query_spec)

            self.assertEqual(query_plan.required_columns, ["close", "volume"])
            self.assertEqual(
                [chunk.chunk_id for chunk in query_plan.candidate_chunks],
                ["000002"],
            )

    def test_group_by_symbol_is_supported(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = self._prepare_table(Path(tmpdir))
            query_spec = build_query_spec(
                table="bars",
                aggregation_tokens=["count"],
                filter_tokens=[],
                group_by_tokens=["symbol"],
            )

            query_plan = build_query_plan(root=root, query_spec=query_spec)

            self.assertEqual(query_plan.required_columns, ["symbol"])
            self.assertEqual(len(query_plan.candidate_chunks), 3)

    def test_cli_query_plan_prints_json(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = self._prepare_table(Path(tmpdir))
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "tickdb.cli",
                    "query-plan",
                    "--table",
                    "bars",
                    "--root",
                    str(root),
                    "--agg",
                    "avg:close",
                    "--filter",
                    "symbol=AAPL",
                ],
                cwd="/Users/abhishekpitale/tickdb",
                check=True,
                capture_output=True,
                text=True,
            )

            payload = json.loads(result.stdout)
            self.assertEqual(payload["table"], "bars")
            self.assertEqual(payload["required_columns"], ["symbol", "close"])
            self.assertEqual(payload["selected_chunk_count"], 1)
            self.assertEqual(payload["candidate_chunks"][0]["chunk_id"], "000000")

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
