from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tickdb.data.generator import generate_csv
from tickdb.storage.wal import ingest_csv_to_wal


class WalIngestionTests(unittest.TestCase):
    def test_ingest_csv_to_wal_appends_rows_and_writes_metadata(self) -> None:
        with TemporaryDirectory() as tmpdir:
            tmp_dir = Path(tmpdir)
            csv_path = tmp_dir / "input.csv"
            root = tmp_dir / ".tickdb"

            generate_csv(
                output_path=csv_path,
                symbols=["AAPL", "MSFT"],
                rows=6,
                start_timestamp=1_700_000_000,
                step_seconds=60,
                seed=7,
            )

            rows_written, wal_path = ingest_csv_to_wal(
                root=root,
                table="bars",
                csv_path=csv_path,
            )

            self.assertEqual(rows_written, 6)
            self.assertTrue(wal_path.exists())

            wal_lines = wal_path.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(wal_lines), 6)

            first_row = json.loads(wal_lines[0])
            self.assertEqual(first_row["symbol"], "AAPL")
            self.assertIsInstance(first_row["timestamp"], int)
            self.assertIsInstance(first_row["close"], float)
            self.assertIsInstance(first_row["volume"], int)

            metadata_path = root / "tables" / "bars" / "metadata" / "table.json"
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            self.assertEqual(metadata["table"], "bars")
            self.assertEqual(metadata["schema"]["close"], "float64")
