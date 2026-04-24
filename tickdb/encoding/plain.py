"""Plain fixed-width binary encodings."""

from __future__ import annotations

import struct
from pathlib import Path
from typing import Sequence


def write_float64_file(path: Path, values: Sequence[float]) -> None:
    path.write_bytes(_pack_values("d", values))


def write_int64_file(path: Path, values: Sequence[int]) -> None:
    path.write_bytes(_pack_values("q", values))


def read_float64_file(path: Path) -> list[float]:
    return [float(value) for value in _unpack_values("d", path.read_bytes())]


def read_int64_file(path: Path) -> list[int]:
    return [int(value) for value in _unpack_values("q", path.read_bytes())]


def _pack_values(type_code: str, values: Sequence[int | float]) -> bytes:
    if not values:
        return b""
    return struct.pack(f"<{len(values)}{type_code}", *values)


def _unpack_values(type_code: str, data: bytes) -> tuple[int | float, ...]:
    if not data:
        return ()
    value_width = struct.calcsize(f"<{type_code}")
    if len(data) % value_width != 0:
        raise ValueError("binary column file has invalid size")
    item_count = len(data) // value_width
    return struct.unpack(f"<{item_count}{type_code}", data)

