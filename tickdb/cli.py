"""Command-line interface for TickDB."""

from __future__ import annotations

import argparse
from pathlib import Path

from tickdb.data.generator import generate_csv
from tickdb.storage.compact import compact_table
from tickdb.storage.wal import ingest_csv_to_wal


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="tickdb",
        description="QuestDB-inspired analytical database for OHLCV market data.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate_parser = subparsers.add_parser(
        "generate", help="Generate synthetic OHLCV data into a CSV file."
    )
    generate_parser.add_argument(
        "--symbols",
        required=True,
        help="Comma-separated symbol list, e.g. AAPL,MSFT,NVDA",
    )
    generate_parser.add_argument("--rows", type=int, required=True)
    generate_parser.add_argument("--output", type=Path, required=True)
    generate_parser.add_argument("--start-ts", type=int, default=1_704_067_200)
    generate_parser.add_argument("--step-seconds", type=int, default=60)
    generate_parser.add_argument("--seed", type=int, default=7)

    ingest_parser = subparsers.add_parser(
        "ingest", help="Append CSV rows into a per-table WAL."
    )
    ingest_parser.add_argument("--table", required=True)
    ingest_parser.add_argument("--file", type=Path, required=True)
    ingest_parser.add_argument("--root", type=Path, default=Path(".tickdb"))

    compact_parser = subparsers.add_parser(
        "compact", help="Compact WAL rows into chunked columnar storage."
    )
    compact_parser.add_argument("--table", required=True)
    compact_parser.add_argument("--root", type=Path, default=Path(".tickdb"))
    compact_parser.add_argument("--chunk-size", type=int, default=10_000)
    compact_parser.add_argument(
        "--layout",
        choices=["time", "symbol_time"],
        default="time",
    )

    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    if args.command == "generate":
        symbols = [symbol.strip() for symbol in args.symbols.split(",") if symbol.strip()]
        rows_written = generate_csv(
            output_path=args.output,
            symbols=symbols,
            rows=args.rows,
            start_timestamp=args.start_ts,
            step_seconds=args.step_seconds,
            seed=args.seed,
        )
        print(f"generated {rows_written} rows at {args.output}")
        return 0

    if args.command == "ingest":
        rows_written, wal_path = ingest_csv_to_wal(
            root=args.root,
            table=args.table,
            csv_path=args.file,
        )
        print(f"ingested {rows_written} rows into {wal_path}")
        return 0

    if args.command == "compact":
        result = compact_table(
            root=args.root,
            table=args.table,
            chunk_size=args.chunk_size,
            layout=args.layout,
        )
        print(
            "compacted "
            f"{result.rows_compacted} rows into {result.chunk_count} chunks at "
            f"{result.manifest_path}"
        )
        return 0

    raise ValueError(f"unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
