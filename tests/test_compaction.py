from __future__ import annotations

import csv
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tickdb.encoding.delta import decode_base_offset_files
from tickdb.encoding.dictionary import decode_dictionary_values, read_dictionary_file
from tickdb.encoding.plain import read_float64_file, read_int64_file
from tickdb.storage.compact import compact_table
from tickdb.storage.wal import ingest_csv_to_wal

FIELDNAMES = ["symbol", "timestamp", "open", "high", "low", "close", "volume"]


class CompactionTests(unittest.TestCase):
    def test_compaction_writes_expected_chunk_files_and_manifest(self) -> None:
        rows = [
            self._row("AAPL", 1000, 10.0, 11.0, 9.5, 10.5, 100),
            self._row("MSFT", 1010, 20.0, 21.0, 19.5, 20.5, 200),
            self._row("NVDA", 1020, 30.0, 31.0, 29.5, 30.5, 300),
            self._row("AAPL", 1030, 11.0, 12.0, 10.5, 11.5, 110),
            self._row("MSFT", 1040, 21.0, 22.0, 20.5, 21.5, 210),
        ]

        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            csv_path = tmp_path / "input.csv"
            root = tmp_path / ".tickdb"
            self._write_csv(csv_path, rows)
            ingest_csv_to_wal(root=root, table="bars", csv_path=csv_path)

            result = compact_table(
                root=root,
                table="bars",
                chunk_size=2,
                layout="time",
            )

            self.assertEqual(result.rows_compacted, 5)
            self.assertEqual(result.chunk_count, 3)
            manifest_path = root / "tables" / "bars" / "metadata" / "chunks.json"
            self.assertEqual(result.manifest_path, manifest_path)
            self.assertTrue(manifest_path.exists())

            chunk_dir = root / "tables" / "bars" / "chunks" / "000000"
            expected_files = {
                "meta.json",
                "symbol.dict.json",
                "symbol.ids.u32",
                "timestamp.base",
                "timestamp.offsets.i64",
                "open.f64",
                "high.f64",
                "low.f64",
                "close.f64",
                "volume.i64",
            }
            self.assertEqual({path.name for path in chunk_dir.iterdir()}, expected_files)

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["layout"], "time")
            self.assertEqual(manifest["chunk_size"], 2)
            self.assertEqual(manifest["chunk_count"], 3)
            self.assertEqual(manifest["total_rows"], 5)

    def test_symbol_time_layout_writes_expected_encodings_and_metadata(self) -> None:
        rows = [
            self._row("MSFT", 1010, 20.0, 21.0, 19.5, 20.5, 200),
            self._row("AAPL", 1020, 11.0, 12.0, 10.5, 11.5, 110),
            self._row("AAPL", 1000, 10.0, 11.0, 9.5, 10.5, 100),
            self._row("MSFT", 1000, 19.0, 20.0, 18.5, 19.5, 190),
        ]

        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            csv_path = tmp_path / "input.csv"
            root = tmp_path / ".tickdb"
            self._write_csv(csv_path, rows)
            ingest_csv_to_wal(root=root, table="bars", csv_path=csv_path)

            compact_table(root=root, table="bars", chunk_size=2, layout="symbol_time")

            chunk_dir = root / "tables" / "bars" / "chunks" / "000000"
            metadata = json.loads((chunk_dir / "meta.json").read_text(encoding="utf-8"))

            self.assertEqual(metadata["chunk_id"], "000000")
            self.assertEqual(metadata["layout"], "symbol_time")
            self.assertEqual(metadata["row_count"], 2)
            self.assertEqual(metadata["symbols"], ["AAPL"])
            self.assertEqual(metadata["timestamp_min"], 1000)
            self.assertEqual(metadata["timestamp_max"], 1020)
            self.assertEqual(metadata["close_min"], 10.5)
            self.assertEqual(metadata["close_max"], 11.5)

            self.assertEqual(read_dictionary_file(chunk_dir / "symbol.dict.json"), ["AAPL"])
            self.assertEqual(
                decode_dictionary_values(
                    chunk_dir / "symbol.dict.json",
                    chunk_dir / "symbol.ids.u32",
                ),
                ["AAPL", "AAPL"],
            )
            self.assertEqual(
                decode_base_offset_files(
                    chunk_dir / "timestamp.base",
                    chunk_dir / "timestamp.offsets.i64",
                ),
                [1000, 1020],
            )
            self.assertEqual(read_float64_file(chunk_dir / "close.f64"), [10.5, 11.5])
            self.assertEqual(read_int64_file(chunk_dir / "volume.i64"), [100, 110])

    def test_time_layout_orders_rows_by_timestamp_then_symbol(self) -> None:
        rows = [
            self._row("NVDA", 1000, 30.0, 31.0, 29.5, 30.5, 300),
            self._row("AAPL", 1000, 10.0, 11.0, 9.5, 10.5, 100),
            self._row("MSFT", 1000, 20.0, 21.0, 19.5, 20.5, 200),
            self._row("AAPL", 1010, 11.0, 12.0, 10.5, 11.5, 110),
        ]

        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            csv_path = tmp_path / "input.csv"
            root = tmp_path / ".tickdb"
            self._write_csv(csv_path, rows)
            ingest_csv_to_wal(root=root, table="bars", csv_path=csv_path)

            compact_table(root=root, table="bars", chunk_size=10, layout="time")

            chunk_dir = root / "tables" / "bars" / "chunks" / "000000"
            self.assertEqual(
                decode_dictionary_values(
                    chunk_dir / "symbol.dict.json",
                    chunk_dir / "symbol.ids.u32",
                ),
                ["AAPL", "MSFT", "NVDA", "AAPL"],
            )
            self.assertEqual(
                decode_base_offset_files(
                    chunk_dir / "timestamp.base",
                    chunk_dir / "timestamp.offsets.i64",
                ),
                [1000, 1000, 1000, 1010],
            )

    @staticmethod
    def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)

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
