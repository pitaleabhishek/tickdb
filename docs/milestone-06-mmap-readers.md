# Milestone 6: mmap-Based Readers

This document explains the Milestone 6 implementation: what `mmap` means in TickDB, what was added, how the reader API works, and why the implementation is intentionally narrow.

## What Milestone 6 Adds

Before this milestone, TickDB could write compacted columnar files but did not yet have a dedicated read API for those files.

After this milestone, TickDB can:

- open fixed-width column files through read-only memory mappings
- derive row counts from file size and type width
- read an entire column
- read a row range from a column
- reconstruct timestamps from `timestamp.base` and `timestamp.offsets.i64`

This is the first real read-side API on top of the storage format created in Milestone 4.

## What `mmap` Means Here

`mmap` means memory-mapped file access.

In practice, TickDB asks the operating system to map a binary column file into memory so the code can access the file bytes directly through a byte buffer instead of repeatedly using ordinary read calls.

For TickDB, that matters because the column files are fixed-width:

- `float64` values are 8 bytes
- `int64` values are 8 bytes

So if a file stores one value per row, row `i` starts at:

```text
offset = i * value_width
```

That makes range-based reads simple and predictable.

## Files Added

This milestone adds:

- `tickdb/storage/mmap_reader.py`
- `tests/test_mmap_reader.py`

No CLI command was added in this milestone. The mmap reader is an internal API that later query execution will use.

## Reader API

The implementation is centered on a small context-managed reader class:

- `FixedWidthMmapReader`
- `Float64MmapReader`
- `Int64MmapReader`
- `TimestampMmapReader`

### `FixedWidthMmapReader`

Responsibilities:

- open a binary file
- create a read-only `mmap`
- validate that file size is a multiple of the element width
- expose:
  - `row_count`
  - `read_all()`
  - `read_range(start, stop)`

The generic reader works from:

- file path
- struct format code
- element width derived from that format code

### `Float64MmapReader`

Thin wrapper around `FixedWidthMmapReader` for:

- `open.f64`
- `high.f64`
- `low.f64`
- `close.f64`

### `Int64MmapReader`

Thin wrapper around `FixedWidthMmapReader` for:

- `volume.i64`
- `timestamp.offsets.i64`

### `TimestampMmapReader`

Small helper that reconstructs timestamps from:

- `timestamp.base`
- `timestamp.offsets.i64`

It uses the `Int64MmapReader` internally and returns:

```text
timestamp = base + offset
```

## Example Usage

```python
from pathlib import Path

from tickdb.storage.mmap_reader import Float64MmapReader, TimestampMmapReader

chunk_dir = Path(".tickdb_demo/tables/bars/chunks/000000")

with Float64MmapReader(chunk_dir / "close.f64") as reader:
    closes = reader.read_range(0, 5)

with TimestampMmapReader(
    chunk_dir / "timestamp.base",
    chunk_dir / "timestamp.offsets.i64",
) as reader:
    timestamps = reader.read_range(0, 5)
```

This is the exact kind of access pattern the future query engine will need.

## Range Semantics

The range API uses Python slice semantics:

- `start` is inclusive
- `stop` is exclusive

Rules:

- `0 <= start <= stop <= row_count`
- empty ranges return `[]`
- invalid ranges raise a clear `ValueError`

That makes the reader easy to reason about and easy to compose with later query planning logic.

## Why the Implementation Is Narrow

This milestone intentionally does not try to solve every future read need.

It does not implement:

- selected row-index reads
- symbol dictionary reads through `mmap`
- query filtering
- aggregation
- pruning logic

The goal is to provide the smallest solid read API needed for the next milestone.

## How It Fits into TickDB

The storage flow is now:

```text
CSV / synthetic data
-> WAL
-> compaction
-> chunked column files
-> mmap-based fixed-width readers
```

That means TickDB now has:

- a write path
- a compaction path
- a basic read path

The next logical step is query planning and execution on top of those readers.

## Testing Coverage

Milestone 6 adds deterministic tests for:

- reading a full `float64` column
- reading a range from a `float64` column
- reading a full `int64` column
- reconstructing timestamps from base + offsets
- validating row counts and rejecting invalid ranges

The tests build a compacted chunk first, then read the actual on-disk files through the mmap reader. So the tests verify real storage integration, not just isolated helper behavior.

## Why This Matters

Milestone 4 made the storage format real.

Milestone 6 makes that storage readable in the way the later query engine actually needs:

- read-only
- range-based
- fixed-width
- explicit

That gives the next query milestone a stable base to build on.
