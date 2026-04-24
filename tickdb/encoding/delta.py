"""Delta-style encodings for ordered integer columns."""

from __future__ import annotations

import struct
from pathlib import Path
from typing import Sequence

from tickdb.encoding.plain import read_int64_file, write_int64_file


def write_base_offset_files(
    base_path: Path,
    offsets_path: Path,
    values: Sequence[int],
) -> tuple[int, list[int]]:
    base, offsets = encode_base_offsets(values)
    base_path.write_bytes(struct.pack("<q", base))
    write_int64_file(offsets_path, offsets)
    return base, offsets


def encode_base_offsets(values: Sequence[int]) -> tuple[int, list[int]]:
    if not values:
        raise ValueError("base+offset encoding requires at least one value")
    base = int(values[0])
    offsets = [int(value) - base for value in values]
    return base, offsets


def read_base_file(path: Path) -> int:
    data = path.read_bytes()
    if len(data) != struct.calcsize("<q"):
        raise ValueError("timestamp base file has invalid size")
    return int(struct.unpack("<q", data)[0])


def decode_base_offset_files(base_path: Path, offsets_path: Path) -> list[int]:
    base = read_base_file(base_path)
    offsets = read_int64_file(offsets_path)
    return [base + int(offset) for offset in offsets]

