"""Metadata helpers for compacted chunk storage."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


@dataclass(frozen=True)
class BlockMetadata:
    block_id: int
    row_start: int
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

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BlockMetadata:
        return cls(
            block_id=int(payload["block_id"]),
            row_start=int(payload["row_start"]),
            row_count=int(payload["row_count"]),
            symbols=[str(symbol) for symbol in payload["symbols"]],
            timestamp_min=int(payload["timestamp_min"]),
            timestamp_max=int(payload["timestamp_max"]),
            open_min=float(payload["open_min"]),
            open_max=float(payload["open_max"]),
            high_min=float(payload["high_min"]),
            high_max=float(payload["high_max"]),
            low_min=float(payload["low_min"]),
            low_max=float(payload["low_max"]),
            close_min=float(payload["close_min"]),
            close_max=float(payload["close_max"]),
            volume_min=int(payload["volume_min"]),
            volume_max=int(payload["volume_max"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "block_id": self.block_id,
            "row_start": self.row_start,
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


@dataclass(frozen=True)
class BlockIndex:
    layout: str
    block_size_rows: int
    blocks: list[BlockMetadata]

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BlockIndex:
        return cls(
            layout=str(payload["layout"]),
            block_size_rows=int(payload["block_size_rows"]),
            blocks=[BlockMetadata.from_dict(block) for block in payload["blocks"]],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "layout": self.layout,
            "block_size_rows": self.block_size_rows,
            "block_count": len(self.blocks),
            "blocks": [block.to_dict() for block in self.blocks],
        }


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

    summary = _build_ohlcv_summary(
        symbols=symbols,
        timestamps=timestamps,
        opens=opens,
        highs=highs,
        lows=lows,
        closes=closes,
        volumes=volumes,
    )

    return ChunkMetadata(
        chunk_id=chunk_id,
        layout=layout,
        row_count=int(summary["row_count"]),
        symbols=list(summary["symbols"]),
        timestamp_min=int(summary["timestamp_min"]),
        timestamp_max=int(summary["timestamp_max"]),
        open_min=float(summary["open_min"]),
        open_max=float(summary["open_max"]),
        high_min=float(summary["high_min"]),
        high_max=float(summary["high_max"]),
        low_min=float(summary["low_min"]),
        low_max=float(summary["low_max"]),
        close_min=float(summary["close_min"]),
        close_max=float(summary["close_max"]),
        volume_min=int(summary["volume_min"]),
        volume_max=int(summary["volume_max"]),
    )


def build_block_index(
    layout: str,
    block_size_rows: int,
    symbols: Sequence[str],
    timestamps: Sequence[int],
    opens: Sequence[float],
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    volumes: Sequence[int],
) -> BlockIndex:
    if block_size_rows <= 0:
        raise ValueError("block_size_rows must be positive")
    if not symbols:
        raise ValueError("cannot build block index for an empty chunk")

    blocks: list[BlockMetadata] = []
    for block_id, row_start in enumerate(range(0, len(symbols), block_size_rows)):
        row_stop = row_start + block_size_rows
        summary = _build_ohlcv_summary(
            symbols=symbols[row_start:row_stop],
            timestamps=timestamps[row_start:row_stop],
            opens=opens[row_start:row_stop],
            highs=highs[row_start:row_stop],
            lows=lows[row_start:row_stop],
            closes=closes[row_start:row_stop],
            volumes=volumes[row_start:row_stop],
        )
        blocks.append(
            BlockMetadata(
                block_id=block_id,
                row_start=row_start,
                row_count=int(summary["row_count"]),
                symbols=list(summary["symbols"]),
                timestamp_min=int(summary["timestamp_min"]),
                timestamp_max=int(summary["timestamp_max"]),
                open_min=float(summary["open_min"]),
                open_max=float(summary["open_max"]),
                high_min=float(summary["high_min"]),
                high_max=float(summary["high_max"]),
                low_min=float(summary["low_min"]),
                low_max=float(summary["low_max"]),
                close_min=float(summary["close_min"]),
                close_max=float(summary["close_max"]),
                volume_min=int(summary["volume_min"]),
                volume_max=int(summary["volume_max"]),
            )
        )

    return BlockIndex(
        layout=layout,
        block_size_rows=block_size_rows,
        blocks=blocks,
    )


def write_chunk_metadata(path: Path, metadata: ChunkMetadata) -> None:
    path.write_text(json.dumps(metadata.to_dict(), indent=2) + "\n", encoding="utf-8")


def write_block_index(path: Path, block_index: BlockIndex) -> None:
    path.write_text(json.dumps(block_index.to_dict(), indent=2) + "\n", encoding="utf-8")


def read_block_index(path: Path) -> BlockIndex:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return BlockIndex.from_dict(payload)


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


def _build_ohlcv_summary(
    symbols: Sequence[str],
    timestamps: Sequence[int],
    opens: Sequence[float],
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    volumes: Sequence[int],
) -> dict[str, Any]:
    if not symbols:
        raise ValueError("cannot build metadata for an empty row set")

    return {
        "row_count": len(symbols),
        "symbols": sorted(set(symbols)),
        "timestamp_min": min(timestamps),
        "timestamp_max": max(timestamps),
        "open_min": min(opens),
        "open_max": max(opens),
        "high_min": min(highs),
        "high_max": max(highs),
        "low_min": min(lows),
        "low_max": max(lows),
        "close_min": min(closes),
        "close_max": max(closes),
        "volume_min": min(volumes),
        "volume_max": max(volumes),
    }
