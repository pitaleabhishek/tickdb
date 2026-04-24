"""WAL-to-columnar compaction."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

from tickdb.encoding.delta import write_base_offset_files
from tickdb.encoding.dictionary import write_dictionary_files
from tickdb.encoding.plain import write_float64_file, write_int64_file
from tickdb.storage.metadata import (
    ChunkMetadata,
    build_chunk_metadata,
    write_chunk_metadata,
    write_chunks_manifest,
)
from tickdb.storage.wal import TablePaths

LAYOUT_TIME = "time"
LAYOUT_SYMBOL_TIME = "symbol_time"
LAYOUT_MODES = {LAYOUT_TIME, LAYOUT_SYMBOL_TIME}


@dataclass(frozen=True)
class BarRow:
    symbol: str
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass(frozen=True)
class CompactionResult:
    table: str
    layout: str
    rows_compacted: int
    chunk_count: int
    manifest_path: Path


def compact_table(
    root: Path,
    table: str,
    chunk_size: int,
    layout: str,
) -> CompactionResult:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    if layout not in LAYOUT_MODES:
        raise ValueError(f"layout must be one of {sorted(LAYOUT_MODES)}, got {layout}")

    paths = TablePaths(root=root, table=table)
    _ensure_compaction_inputs(paths)
    _ensure_compaction_outputs(paths)

    rows = _load_wal_rows(paths)
    if not rows:
        raise ValueError(f"table {table!r} has no WAL rows to compact")

    sorted_rows = _sort_rows(rows, layout)
    chunk_metadatas: list[ChunkMetadata] = []

    paths.chunks_dir.mkdir(parents=True, exist_ok=True)
    for chunk_index, chunk_rows in enumerate(_iter_chunks(sorted_rows, chunk_size)):
        chunk_id = f"{chunk_index:06d}"
        chunk_metadatas.append(
            _write_chunk(
                chunk_dir=paths.chunks_dir / chunk_id,
                chunk_id=chunk_id,
                layout=layout,
                rows=chunk_rows,
            )
        )

    write_chunks_manifest(
        path=paths.chunks_metadata_path,
        table=table,
        layout=layout,
        chunk_size=chunk_size,
        chunks=chunk_metadatas,
    )

    return CompactionResult(
        table=table,
        layout=layout,
        rows_compacted=len(sorted_rows),
        chunk_count=len(chunk_metadatas),
        manifest_path=paths.chunks_metadata_path,
    )


def _ensure_compaction_inputs(paths: TablePaths) -> None:
    if not paths.table_metadata_path.exists():
        raise FileNotFoundError(
            f"table metadata does not exist for {paths.table!r}: {paths.table_metadata_path}"
        )
    if not paths.wal_dir.exists():
        raise FileNotFoundError(f"WAL directory does not exist: {paths.wal_dir}")
    if not list(_discover_wal_paths(paths)):
        raise FileNotFoundError(f"no WAL segments found under {paths.wal_dir}")


def _ensure_compaction_outputs(paths: TablePaths) -> None:
    if paths.chunks_metadata_path.exists():
        raise FileExistsError(
            f"chunk manifest already exists for table {paths.table!r}: {paths.chunks_metadata_path}"
        )
    if paths.chunks_dir.exists() and any(paths.chunks_dir.iterdir()):
        raise FileExistsError(
            f"chunk output already exists for table {paths.table!r}: {paths.chunks_dir}"
        )


def _discover_wal_paths(paths: TablePaths) -> Iterable[Path]:
    return sorted(paths.wal_dir.glob("*.jsonl"))


def _load_wal_rows(paths: TablePaths) -> list[BarRow]:
    rows: list[BarRow] = []
    for wal_path in _discover_wal_paths(paths):
        with wal_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                payload = json.loads(line)
                rows.append(
                    BarRow(
                        symbol=str(payload["symbol"]),
                        timestamp=int(payload["timestamp"]),
                        open=float(payload["open"]),
                        high=float(payload["high"]),
                        low=float(payload["low"]),
                        close=float(payload["close"]),
                        volume=int(payload["volume"]),
                    )
                )
    return rows


def _sort_rows(rows: Sequence[BarRow], layout: str) -> list[BarRow]:
    if layout == LAYOUT_TIME:
        return sorted(rows, key=lambda row: (row.timestamp, row.symbol))
    if layout == LAYOUT_SYMBOL_TIME:
        return sorted(rows, key=lambda row: (row.symbol, row.timestamp))
    raise ValueError(f"unsupported layout: {layout}")


def _iter_chunks(rows: Sequence[BarRow], chunk_size: int) -> Iterable[list[BarRow]]:
    for start in range(0, len(rows), chunk_size):
        yield list(rows[start : start + chunk_size])


def _write_chunk(
    chunk_dir: Path,
    chunk_id: str,
    layout: str,
    rows: Sequence[BarRow],
) -> ChunkMetadata:
    chunk_dir.mkdir(parents=True, exist_ok=False)

    symbols = [row.symbol for row in rows]
    timestamps = [row.timestamp for row in rows]
    opens = [row.open for row in rows]
    highs = [row.high for row in rows]
    lows = [row.low for row in rows]
    closes = [row.close for row in rows]
    volumes = [row.volume for row in rows]

    write_dictionary_files(
        dictionary_path=chunk_dir / "symbol.dict.json",
        ids_path=chunk_dir / "symbol.ids.u32",
        values=symbols,
    )
    write_base_offset_files(
        base_path=chunk_dir / "timestamp.base",
        offsets_path=chunk_dir / "timestamp.offsets.i64",
        values=timestamps,
    )
    write_float64_file(chunk_dir / "open.f64", opens)
    write_float64_file(chunk_dir / "high.f64", highs)
    write_float64_file(chunk_dir / "low.f64", lows)
    write_float64_file(chunk_dir / "close.f64", closes)
    write_int64_file(chunk_dir / "volume.i64", volumes)

    metadata = build_chunk_metadata(
        chunk_id=chunk_id,
        layout=layout,
        symbols=symbols,
        timestamps=timestamps,
        opens=opens,
        highs=highs,
        lows=lows,
        closes=closes,
        volumes=volumes,
    )
    write_chunk_metadata(chunk_dir / "meta.json", metadata)
    return metadata

