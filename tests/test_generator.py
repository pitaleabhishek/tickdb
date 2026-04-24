from __future__ import annotations

import csv
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tickdb.data.generator import FIELDNAMES, generate_csv


class GenerateCsvTests(unittest.TestCase):
    def test_generate_csv_writes_expected_header_and_rows(self) -> None:
        with TemporaryDirectory() as tmpdir:
            tmp_dir = Path(tmpdir)
            output_path = tmp_dir / "sample.csv"

            rows_written = generate_csv(
                output_path=output_path,
                symbols=["AAPL", "MSFT"],
                rows=12,
                start_timestamp=1_700_000_000,
                step_seconds=60,
                seed=7,
            )

            self.assertEqual(rows_written, 12)

            with output_path.open("r", newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                rows = list(reader)

            self.assertEqual(reader.fieldnames, FIELDNAMES)
            self.assertEqual(len(rows), 12)
            self.assertEqual(rows[0]["symbol"], "AAPL")
            self.assertEqual(rows[1]["symbol"], "MSFT")
