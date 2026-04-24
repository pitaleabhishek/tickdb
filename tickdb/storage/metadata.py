"""Metadata helpers for compacted chunk storage."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


@dataclass(frozen=True)
class ChunkMetadata:
    chunk_id: str
    layout: str
    row_count: int
    symbols: list[str]
    timestamp_min: int
    timestamp_max: int
    open_min: float
    open_max: float
    high_min: float
    high_max: float
    low_min: float
    low_max: float
    close_min: float
    close_max: float
    volume_min: int
    volume_max: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "chunk_id": self.chunk_id,
            "layout": self.layout,
            "row_count": self.row_count,
            "symbols": self.symbols,
            "timestamp_min": self.timestamp_min,
            "timestamp_max": self.timestamp_max,
            "open_min": self.open_min,
            "open_max": self.open_max,
            "high_min": self.high_min,
            "high_max": self.high_max,
            "low_min": self.low_min,
            "low_max": self.low_max,
            "close_min": self.close_min,
            "close_max": self.close_max,
            "volume_min": self.volume_min,
            "volume_max": self.volume_max,
        }

    def to_manifest_entry(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload["path"] = f"chunks/{self.chunk_id}"
        return payload


def build_chunk_metadata(
    chunk_id: str,
    layout: str,
    symbols: Sequence[str],
    timestamps: Sequence[int],
    opens: Sequence[float],
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    volumes: Sequence[int],
) -> ChunkMetadata:
    if not symbols:
        raise ValueError("cannot build metadata for an empty chunk")

    return ChunkMetadata(
        chunk_id=chunk_id,
        layout=layout,
        row_count=len(symbols),
        symbols=sorted(set(symbols)),
        timestamp_min=min(timestamps),
        timestamp_max=max(timestamps),
        open_min=min(opens),
        open_max=max(opens),
        high_min=min(highs),
        high_max=max(highs),
        low_min=min(lows),
        low_max=max(lows),
        close_min=min(closes),
        close_max=max(closes),
        volume_min=min(volumes),
        volume_max=max(volumes),
    )


def write_chunk_metadata(path: Path, metadata: ChunkMetadata) -> None:
    path.write_text(json.dumps(metadata.to_dict(), indent=2) + "\n", encoding="utf-8")


def write_chunks_manifest(
    path: Path,
    table: str,
    layout: str,
    chunk_size: int,
    chunks: Sequence[ChunkMetadata],
) -> None:
    payload = {
        "table": table,
        "layout": layout,
        "chunk_size": chunk_size,
        "chunk_count": len(chunks),
        "total_rows": sum(chunk.row_count for chunk in chunks),
        "chunks": [chunk.to_manifest_entry() for chunk in chunks],
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

