"""Write-ahead logging utilities."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

OHLCV_FIELDS = ["symbol", "timestamp", "open", "high", "low", "close", "volume"]


@dataclass(frozen=True)
class TablePaths:
    root: Path
    table: str

    @property
    def table_root(self) -> Path:
        return self.root / "tables" / self.table

    @property
    def wal_dir(self) -> Path:
        return self.table_root / "wal"

    @property
    def metadata_dir(self) -> Path:
        return self.table_root / "metadata"

    @property
    def table_metadata_path(self) -> Path:
        return self.metadata_dir / "table.json"

    @property
    def wal_path(self) -> Path:
        return self.wal_dir / "000001.jsonl"


def ingest_csv_to_wal(root: Path, table: str, csv_path: Path) -> tuple[int, Path]:
    """Read OHLCV rows from CSV and append them into a table WAL segment."""
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    paths = TablePaths(root=root, table=table)
    _ensure_table_layout(paths)
    _write_table_metadata(paths)

    rows_written = 0
    with csv_path.open("r", newline="", encoding="utf-8") as source, paths.wal_path.open(
        "a", encoding="utf-8"
    ) as wal_handle:
        reader = csv.DictReader(source)
        _validate_csv_header(reader.fieldnames)

        for raw_row in reader:
            row = _normalize_row(raw_row)
            wal_handle.write(json.dumps(row, separators=(",", ":")) + "\n")
            rows_written += 1

    return rows_written, paths.wal_path


def _ensure_table_layout(paths: TablePaths) -> None:
    paths.wal_dir.mkdir(parents=True, exist_ok=True)
    paths.metadata_dir.mkdir(parents=True, exist_ok=True)


def _write_table_metadata(paths: TablePaths) -> None:
    if paths.table_metadata_path.exists():
        return

    metadata = {
        "table": paths.table,
        "schema": {
            "symbol": "string",
            "timestamp": "int64",
            "open": "float64",
            "high": "float64",
            "low": "float64",
            "close": "float64",
            "volume": "int64",
        },
        "created_at": datetime.now(UTC).isoformat(),
    }
    paths.table_metadata_path.write_text(
        json.dumps(metadata, indent=2) + "\n",
        encoding="utf-8",
    )


def _validate_csv_header(fieldnames: list[str] | None) -> None:
    if fieldnames is None:
        raise ValueError("CSV file is missing a header row")
    if fieldnames != OHLCV_FIELDS:
        raise ValueError(
            f"CSV columns must exactly match {OHLCV_FIELDS}, got {fieldnames}"
        )


def _normalize_row(raw_row: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": str(raw_row["symbol"]),
        "timestamp": int(raw_row["timestamp"]),
        "open": float(raw_row["open"]),
        "high": float(raw_row["high"]),
        "low": float(raw_row["low"]),
        "close": float(raw_row["close"]),
        "volume": int(raw_row["volume"]),
    }

