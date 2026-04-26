from __future__ import annotations

import csv
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tickdb.storage.compact import compact_table
from tickdb.storage.mmap_reader import (
    Float64MmapReader,
    Int64MmapReader,
    TimestampMmapReader,
    UInt32MmapReader,
)
from tickdb.storage.wal import ingest_csv_to_wal

FIELDNAMES = ["symbol", "timestamp", "open", "high", "low", "close", "volume"]


class MmapReaderTests(unittest.TestCase):
    def test_float64_reader_reads_full_column(self) -> None:
        chunk_dir = self._prepare_chunk()

        with Float64MmapReader(chunk_dir / "close.f64") as reader:
            self.assertEqual(reader.row_count, 4)
            self.assertEqual(reader.read_all(), [10.5, 11.5, 12.5, 13.5])

    def test_float64_reader_reads_row_range(self) -> None:
        chunk_dir = self._prepare_chunk()

        with Float64MmapReader(chunk_dir / "close.f64") as reader:
            self.assertEqual(reader.read_range(1, 3), [11.5, 12.5])
            self.assertEqual(reader.read_range(2, 2), [])
            self.assertEqual(len(reader.read_range_bytes(1, 3)), 16)

    def test_int64_reader_reads_full_column(self) -> None:
        chunk_dir = self._prepare_chunk()

        with Int64MmapReader(chunk_dir / "volume.i64") as reader:
            self.assertEqual(reader.row_count, 4)
            self.assertEqual(reader.read_all(), [100, 120, 130, 140])

    def test_uint32_reader_reads_symbol_id_range(self) -> None:
        chunk_dir = self._prepare_chunk()

        with UInt32MmapReader(chunk_dir / "symbol.ids.u32") as reader:
            self.assertEqual(reader.row_count, 4)
            self.assertEqual(reader.read_range(1, 3), [0, 0])

    def test_timestamp_reader_reconstructs_values(self) -> None:
        chunk_dir = self._prepare_chunk()

        with TimestampMmapReader(
            chunk_dir / "timestamp.base",
            chunk_dir / "timestamp.offsets.i64",
        ) as reader:
            self.assertEqual(reader.row_count, 4)
            self.assertEqual(reader.read_all(), [1000, 1010, 1020, 1030])
            self.assertEqual(reader.read_range(1, 3), [1010, 1020])
            self.assertEqual(len(reader.read_range_offset_bytes(1, 3)), 16)

    def test_reader_rejects_invalid_ranges(self) -> None:
        chunk_dir = self._prepare_chunk()

        with Float64MmapReader(chunk_dir / "close.f64") as reader:
            with self.assertRaisesRegex(ValueError, "non-negative"):
                reader.read_range(-1, 2)
            with self.assertRaisesRegex(ValueError, "start must be <= stop"):
                reader.read_range(3, 2)
            with self.assertRaisesRegex(ValueError, "exceeds row_count"):
                reader.read_range(0, 10)

    def _prepare_chunk(self) -> Path:
        rows = [
            self._row("AAPL", 1000, 10.0, 11.0, 9.5, 10.5, 100),
            self._row("AAPL", 1010, 10.5, 12.0, 10.0, 11.5, 120),
            self._row("AAPL", 1020, 11.5, 13.0, 11.0, 12.5, 130),
            self._row("AAPL", 1030, 12.5, 14.0, 12.0, 13.5, 140),
        ]

        tmpdir = TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        tmp_path = Path(tmpdir.name)
        csv_path = tmp_path / "input.csv"
        root = tmp_path / ".tickdb"

        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)

        ingest_csv_to_wal(root=root, table="bars", csv_path=csv_path)
        compact_table(root=root, table="bars", chunk_size=10, layout="symbol_time")
        return root / "tables" / "bars" / "chunks" / "000000"

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
