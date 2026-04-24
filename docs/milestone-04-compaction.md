# Milestone 4: Columnar Compaction

This document explains the Milestone 4 implementation in concrete terms: what was added, how the compaction pipeline works, how the on-disk format is written, and how the code is structured.

## What Milestone 4 Adds

Before this milestone, TickDB could:

- generate synthetic OHLCV CSV data
- ingest CSV rows into a per-table WAL
- write minimal table metadata

After this milestone, TickDB can also:

- read WAL rows back from disk
- reorder them into a chosen physical layout
- split them into fixed-size chunks
- write compacted per-chunk column files
- write per-chunk pruning metadata
- write a table-level chunk manifest

That is the point where TickDB stops being only an ingest pipeline and starts becoming a real analytical storage engine.

## Command Surface

Milestone 4 adds:

```bash
tickdb compact \
  --table bars \
  --root .tickdb \
  --chunk-size 10000 \
  --layout symbol_time
```

Arguments:

- `--table`: table name to compact
- `--root`: TickDB root directory
- `--chunk-size`: number of rows per output chunk
- `--layout`: one of `time` or `symbol_time`

The CLI only parses arguments and dispatches into the storage layer. All real work happens in `tickdb/storage/compact.py`.

## High-Level Pipeline

The compaction pipeline is:

1. validate inputs
2. load WAL rows
3. sort rows into the chosen physical layout
4. split rows into chunks
5. write encoded column files for each chunk
6. compute and write chunk metadata
7. write the table-level chunk manifest

In short:

```text
WAL rows -> sorted rows -> chunks -> column files + metadata
```

## Code Structure

The implementation is intentionally split into small modules.

### `tickdb/cli.py`

Responsibilities:

- define the `compact` command
- parse CLI arguments
- call `compact_table(...)`
- print a concise success message

### `tickdb/storage/compact.py`

Responsibilities:

- validate compaction inputs and outputs
- discover WAL segment files
- load WAL rows into typed in-memory records
- sort rows according to layout mode
- split rows into fixed-size chunks
- coordinate file writing for each chunk
- coordinate manifest writing

Key types:

- `BarRow`: typed in-memory representation of one OHLCV row
- `CompactionResult`: summary returned to the CLI

### `tickdb/storage/metadata.py`

Responsibilities:

- define the chunk metadata structure
- compute min/max statistics and symbol sets
- write per-chunk `meta.json`
- write the table-level `metadata/chunks.json`

### `tickdb/encoding/plain.py`

Responsibilities:

- write fixed-width binary numeric column files
- read them back for tests

Used for:

- `open.f64`
- `high.f64`
- `low.f64`
- `close.f64`
- `volume.i64`

### `tickdb/encoding/dictionary.py`

Responsibilities:

- dictionary-encode symbol strings
- write the human-readable dictionary file
- write encoded symbol ids
- decode them for tests

Used for:

- `symbol.dict.json`
- `symbol.ids.u32`

### `tickdb/encoding/delta.py`

Responsibilities:

- encode timestamps as `base + offsets`
- write base and offsets files
- reconstruct timestamps for tests

Used for:

- `timestamp.base`
- `timestamp.offsets.i64`

## Input Validation

Compaction refuses to run unless:

- `metadata/table.json` exists
- the table WAL directory exists
- at least one WAL segment file exists

Compaction also refuses to overwrite existing compacted output. If either of these already exists, it fails:

- `metadata/chunks.json`
- a non-empty `chunks/` directory

This is deliberate. The first implementation is conservative and avoids destructive replacement behavior.

## WAL Loading

Compaction reads JSONL rows from the table WAL directory.

Current behavior:

- discover files matching `*.jsonl` under `wal/`
- read them in sorted filename order
- parse each line as JSON
- convert each record into a typed `BarRow`

Even though the current ingest implementation writes only `000001.jsonl`, compaction is already written to discover WAL files by directory scan rather than hardcoding a single path.

## Layout Modes

Milestone 4 supports two physical layouts.

### `time`

Sort key:

- `timestamp`
- `symbol`

Effect:

- strong time locality
- chunks tend to mix symbols more often

### `symbol_time`

Sort key:

- `symbol`
- `timestamp`

Effect:

- stronger symbol locality
- tighter symbol sets per chunk on average
- useful foundation for later symbol pruning and optional symbol RLE

Important detail:

`symbol_time` does not guarantee one symbol per chunk. It only guarantees that rows are sorted by symbol before chunking. Fixed chunk boundaries can still create mixed-symbol chunks.

## Chunk Splitting

After rows are sorted, the code slices them into fixed-size chunks.

Example:

- 20 rows total
- `chunk_size = 5`

Output:

- `000000`
- `000001`
- `000002`
- `000003`

The final chunk may be smaller than `chunk_size`.

## What Gets Written Per Chunk

Assume one chunk contains these rows:

```text
AAPL 1000 open=10.0 high=11.0 low=9.5 close=10.5 volume=100
AAPL 1010 open=10.5 high=12.0 low=10.0 close=11.5 volume=120
AAPL 1020 open=11.5 high=13.0 low=11.0 close=12.5 volume=130
```

The chunk directory becomes:

```text
chunks/
  000000/
    meta.json
    symbol.dict.json
    symbol.ids.u32
    timestamp.base
    timestamp.offsets.i64
    open.f64
    high.f64
    low.f64
    close.f64
    volume.i64
```

Logical contents:

- `symbol.dict.json` -> `["AAPL"]`
- `symbol.ids.u32` -> `[0, 0, 0]`
- `timestamp.base` -> `1000`
- `timestamp.offsets.i64` -> `[0, 10, 20]`
- `open.f64` -> `[10.0, 10.5, 11.5]`
- `high.f64` -> `[11.0, 12.0, 13.0]`
- `low.f64` -> `[9.5, 10.0, 11.0]`
- `close.f64` -> `[10.5, 11.5, 12.5]`
- `volume.i64` -> `[100, 120, 130]`

All column files use the same row order within the chunk. Position `i` across all files refers to the same logical row.

## Symbol Encoding

The symbol column is encoded chunk-locally.

Example input:

```text
AAPL, AAPL, MSFT, AAPL
```

Chunk-local dictionary:

```text
0 -> AAPL
1 -> MSFT
```

Encoded ids:

```text
[0, 0, 1, 0]
```

Files written:

- `symbol.dict.json`
- `symbol.ids.u32`

The dictionary file is human-readable to make inspection and debugging easy.

## Timestamp Encoding

Timestamps are encoded as:

```text
base timestamp + int64 offsets
```

Example input:

```text
[1704067200, 1704067260, 1704067320]
```

Written as:

- `timestamp.base` -> `1704067200`
- `timestamp.offsets.i64` -> `[0, 60, 120]`

This keeps the on-disk representation simple while preserving exact reconstruction.

## Numeric Column Encoding

Numeric columns are written as fixed-width little-endian binary buffers.

Formats:

- `open/high/low/close` -> `float64`
- `volume` -> `int64`

Why:

- predictable offsets
- simple writing code
- future-friendly for `mmap`

## Chunk Metadata

Each chunk gets a `meta.json` file containing:

- `chunk_id`
- `layout`
- `row_count`
- `symbols`
- `timestamp_min`, `timestamp_max`
- `open_min`, `open_max`
- `high_min`, `high_max`
- `low_min`, `low_max`
- `close_min`, `close_max`
- `volume_min`, `volume_max`

Example:

```json
{
  "chunk_id": "000000",
  "layout": "symbol_time",
  "row_count": 3,
  "symbols": ["AAPL"],
  "timestamp_min": 1000,
  "timestamp_max": 1020,
  "close_min": 10.5,
  "close_max": 12.5,
  "volume_min": 100,
  "volume_max": 130
}
```

This metadata is the basis for later chunk pruning.

## Table-Level Manifest

Milestone 4 also writes:

```text
metadata/chunks.json
```

This is a table-level manifest that contains:

- table name
- layout
- chunk size
- chunk count
- total rows
- a summary entry for each chunk

This avoids requiring later query code to walk every chunk directory before planning.

## Example End-to-End Run

Commands:

```bash
python3 -m tickdb.cli generate --symbols AAPL,MSFT,NVDA --rows 20 --output data/sample_ohlcv.csv
python3 -m tickdb.cli ingest --table bars --file data/sample_ohlcv.csv --root .tickdb_demo
python3 -m tickdb.cli compact --table bars --root .tickdb_demo --chunk-size 5 --layout symbol_time
```

Example result:

```text
compacted 20 rows into 4 chunks at .tickdb_demo/tables/bars/metadata/chunks.json
```

Meaning:

- 20 WAL rows were loaded
- rows were sorted using `symbol_time`
- rows were split into 4 chunks of 5 rows each
- chunk files were written under `.tickdb_demo/tables/bars/chunks/`
- the manifest was written to `.tickdb_demo/tables/bars/metadata/chunks.json`

## Testing Coverage

Milestone 4 adds deterministic tests for:

- chunk files and manifest creation
- `symbol_time` layout correctness
- dictionary and timestamp decoding round trips
- numeric file round trips
- `time` layout ordering

The tests verify actual on-disk output, not just in-memory helper behavior.


