"""Read-only mmap-based readers for fixed-width column files."""

from __future__ import annotations

import mmap
import struct
from pathlib import Path
from typing import BinaryIO


class FixedWidthMmapReader:
    """Read a fixed-width binary column file through a read-only mmap."""

    def __init__(self, path: Path, format_code: str) -> None:
        self.path = path
        self.format_code = format_code
        self.value_width = struct.calcsize(f"<{format_code}")
        self._file: BinaryIO | None = None
        self._mapping: mmap.mmap | None = None
        self._row_count = 0

    def __enter__(self) -> FixedWidthMmapReader:
        self._file = self.path.open("rb")
        self._mapping = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
        self._row_count = self._compute_row_count()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._mapping is not None:
            self._mapping.close()
            self._mapping = None
        if self._file is not None:
            self._file.close()
            self._file = None

    @property
    def row_count(self) -> int:
        self._ensure_open()
        return self._row_count

    def read_all(self) -> list[int | float]:
        return self.read_range(0, self.row_count)

    def read_all_bytes(self) -> bytes:
        return self.read_range_bytes(0, self.row_count)

    def read_range(self, start: int, stop: int) -> list[int | float]:
        self._ensure_open()
        self._validate_range(start, stop)
        if start == stop:
            return []

        byte_start = start * self.value_width
        byte_stop = stop * self.value_width
        raw = self._mapping[byte_start:byte_stop]
        count = stop - start
        return list(struct.unpack(f"<{count}{self.format_code}", raw))

    def read_range_bytes(self, start: int, stop: int) -> bytes:
        self._ensure_open()
        self._validate_range(start, stop)
        if start == stop:
            return b""

        byte_start = start * self.value_width
        byte_stop = stop * self.value_width
        return self._mapping[byte_start:byte_stop]

    def _compute_row_count(self) -> int:
        size = len(self._mapping)
        if size % self.value_width != 0:
            raise ValueError(
                f"file size for {self.path} is not a multiple of element width {self.value_width}"
            )
        return size // self.value_width

    def _validate_range(self, start: int, stop: int) -> None:
        if start < 0 or stop < 0:
            raise ValueError("range indices must be non-negative")
        if start > stop:
            raise ValueError("range start must be <= stop")
        if stop > self.row_count:
            raise ValueError(
                f"range stop {stop} exceeds row_count {self.row_count} for {self.path}"
            )

    def _ensure_open(self) -> None:
        if self._mapping is None:
            raise ValueError("mmap reader is not open")


class Float64MmapReader(FixedWidthMmapReader):
    def __init__(self, path: Path) -> None:
        super().__init__(path=path, format_code="d")


class Int64MmapReader(FixedWidthMmapReader):
    def __init__(self, path: Path) -> None:
        super().__init__(path=path, format_code="q")


class UInt32MmapReader(FixedWidthMmapReader):
    def __init__(self, path: Path) -> None:
        super().__init__(path=path, format_code="I")


class TimestampMmapReader:
    """Reconstruct timestamps from base + int64 offsets."""

    def __init__(self, base_path: Path, offsets_path: Path) -> None:
        self.base_path = base_path
        self.offsets_path = offsets_path
        self.base = 0
        self._offset_reader = Int64MmapReader(offsets_path)

    def __enter__(self) -> TimestampMmapReader:
        self.base = _read_base_int64(self.base_path)
        self._offset_reader.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._offset_reader.__exit__(exc_type, exc, tb)

    @property
    def row_count(self) -> int:
        return self._offset_reader.row_count

    @property
    def base_value(self) -> int:
        return self.base

    def read_all(self) -> list[int]:
        return self.read_range(0, self.row_count)

    def read_all_offset_bytes(self) -> bytes:
        return self.read_range_offset_bytes(0, self.row_count)

    def read_range(self, start: int, stop: int) -> list[int]:
        offsets = self._offset_reader.read_range(start, stop)
        return [self.base + int(offset) for offset in offsets]

    def read_range_offset_bytes(self, start: int, stop: int) -> bytes:
        return self._offset_reader.read_range_bytes(start, stop)


def _read_base_int64(path: Path) -> int:
    data = path.read_bytes()
    if len(data) != struct.calcsize("<q"):
        raise ValueError(f"timestamp base file has invalid size: {path}")
    return int(struct.unpack("<q", data)[0])
