"""Benchmark chunk-only pruning versus chunk plus block pruning."""

from __future__ import annotations

import argparse
import json
import platform
import shutil
import statistics
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from benchmarks.query_cases import (
    BenchmarkCase,
    BenchmarkContext,
    build_block_index_cases,
)
from tickdb.data.generator import generate_csv
from tickdb.query.executor import execute_query
from tickdb.query.parser import build_query_spec
from tickdb.storage.compact import compact_table
from tickdb.storage.wal import ingest_csv_to_wal

DEFAULT_SYMBOLS = [
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "GOOG",
    "META",
    "TSLA",
    "AMD",
    "AVGO",
    "SPY",
]


@dataclass(frozen=True)
class BenchmarkConfig:
    rows: int
    chunk_size: int
    fine_block_size_rows: int
    warmup_runs: int
    measured_runs: int
    seed: int
    step_seconds: int
    start_timestamp: int
    symbols: list[str]


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run TickDB block-index comparison benchmarks."
    )
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--chunk-size", type=int, default=10_000)
    parser.add_argument("--fine-block-size-rows", type=int, default=1_024)
    parser.add_argument("--warmup-runs", type=int, default=1)
    parser.add_argument("--measured-runs", type=int, default=3)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--step-seconds", type=int, default=60)
    parser.add_argument("--start-ts", type=int, default=1_704_067_200)
    parser.add_argument(
        "--symbols",
        default=",".join(DEFAULT_SYMBOLS),
        help="Comma-separated symbol list for data generation.",
    )
    parser.add_argument(
        "--artifacts-root",
        type=Path,
        default=Path("benchmarks/.artifacts/block-pruning"),
    )
    parser.add_argument(
        "--results-root",
        type=Path,
        default=Path("benchmarks/results/block-pruning"),
    )
    parser.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Delete existing benchmark artifacts and rebuild from scratch.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    symbols = [symbol.strip() for symbol in args.symbols.split(",") if symbol.strip()]
    config = BenchmarkConfig(
        rows=args.rows,
        chunk_size=args.chunk_size,
        fine_block_size_rows=args.fine_block_size_rows,
        warmup_runs=args.warmup_runs,
        measured_runs=args.measured_runs,
        seed=args.seed,
        step_seconds=args.step_seconds,
        start_timestamp=args.start_ts,
        symbols=symbols,
    )

    artifacts_root = args.artifacts_root / f"{config.rows}_rows"
    if args.force_rebuild and artifacts_root.exists():
        shutil.rmtree(artifacts_root)

    dataset_path = artifacts_root / "benchmark_ohlcv.csv"
    roots = {
        "time": {
            "coarse": artifacts_root / "time_coarse_root",
            "fine": artifacts_root / "time_fine_root",
        },
        "symbol_time": {
            "coarse": artifacts_root / "symbol_time_coarse_root",
            "fine": artifacts_root / "symbol_time_fine_root",
        },
    }
    _prepare_benchmark_storage(config=config, dataset_path=dataset_path, roots=roots)

    context = BenchmarkContext(
        min_timestamp=config.start_timestamp,
        max_timestamp=config.start_timestamp + ((config.rows - 1) * config.step_seconds),
    )
    cases = build_block_index_cases(context)
    results = _run_benchmark_matrix(config=config, cases=cases, roots=roots)

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "config": asdict(config),
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
        },
        "cases": results,
    }

    results_root = args.results_root
    results_root.mkdir(parents=True, exist_ok=True)
    json_path = results_root / f"{_row_label(config.rows)}.json"
    md_path = results_root / f"{_row_label(config.rows)}.md"
    json_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown_report(payload), encoding="utf-8")

    print(f"saved benchmark JSON to {json_path}")
    print(f"saved benchmark report to {md_path}")
    print(_render_console_summary(payload))
    return 0


def _prepare_benchmark_storage(
    config: BenchmarkConfig,
    dataset_path: Path,
    roots: dict[str, dict[str, Path]],
) -> None:
    if not dataset_path.exists():
        generate_csv(
            output_path=dataset_path,
            symbols=config.symbols,
            rows=config.rows,
            start_timestamp=config.start_timestamp,
            step_seconds=config.step_seconds,
            seed=config.seed,
        )

    for layout, layout_roots in roots.items():
        for block_mode, root in layout_roots.items():
            manifest_path = root / "tables" / "bars" / "metadata" / "chunks.json"
            if manifest_path.exists():
                continue

            if root.exists():
                shutil.rmtree(root)

            ingest_csv_to_wal(root=root, table="bars", csv_path=dataset_path)
            compact_table(
                root=root,
                table="bars",
                chunk_size=config.chunk_size,
                layout=layout,
                block_size_rows=(
                    config.chunk_size
                    if block_mode == "coarse"
                    else config.fine_block_size_rows
                ),
            )


def _run_benchmark_matrix(
    config: BenchmarkConfig,
    cases: list[BenchmarkCase],
    roots: dict[str, dict[str, Path]],
) -> list[dict[str, Any]]:
    case_results: list[dict[str, Any]] = []
    for case in cases:
        layout_results = {
            layout: {
                block_mode: _run_case_for_root(
                    root=root,
                    case=case,
                    warmup_runs=config.warmup_runs,
                    measured_runs=config.measured_runs,
                )
                for block_mode, root in layout_roots.items()
            }
            for layout, layout_roots in roots.items()
        }
        case_results.append(
            {
                "name": case.name,
                "title": case.title,
                "query_sql": case.query_sql,
                "description": case.description,
                "business_use_case": case.business_use_case,
                "aggregation_tokens": case.aggregation_tokens,
                "filter_tokens": case.filter_tokens,
                "group_by_tokens": case.group_by_tokens,
                "layouts": layout_results,
            }
        )
    return case_results


def _run_case_for_root(
    root: Path,
    case: BenchmarkCase,
    warmup_runs: int,
    measured_runs: int,
) -> dict[str, Any]:
    query_spec = build_query_spec(
        table="bars",
        aggregation_tokens=case.aggregation_tokens,
        filter_tokens=case.filter_tokens,
        group_by_tokens=case.group_by_tokens,
    )

    for _ in range(warmup_runs):
        execute_query(root=root, query_spec=query_spec)

    durations_ms: list[float] = []
    final_payload: dict[str, Any] | None = None
    for _ in range(measured_runs):
        started = time.perf_counter()
        result = execute_query(root=root, query_spec=query_spec)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        durations_ms.append(elapsed_ms)
        final_payload = result.to_dict()

    assert final_payload is not None
    final_payload["runtime_ms"] = {
        "runs": [round(duration, 3) for duration in durations_ms],
        "median": round(statistics.median(durations_ms), 3),
        "min": round(min(durations_ms), 3),
        "max": round(max(durations_ms), 3),
    }
    return final_payload


def _render_markdown_report(payload: dict[str, Any]) -> str:
    lines = [
        "# Block Index Comparison Benchmark",
        "",
        f"Generated: {payload['generated_at']}",
        "",
        "## Configuration",
        "",
        f"- rows: `{payload['config']['rows']}`",
        f"- chunk_size: `{payload['config']['chunk_size']}`",
        f"- fine_block_size_rows: `{payload['config']['fine_block_size_rows']}`",
        f"- symbols: `{', '.join(payload['config']['symbols'])}`",
        f"- warmup_runs: `{payload['config']['warmup_runs']}`",
        f"- measured_runs: `{payload['config']['measured_runs']}`",
        "",
        "Coarse mode sets `block_size_rows = chunk_size`, which makes block pruning equivalent to chunk-only scanning.",
        "",
    ]

    for case in payload["cases"]:
        improvement_lines: list[str] = []
        lines.extend(
            [
                f"## {case['title']}",
                "",
                f"`{case['query_sql']}`",
                "",
                case["description"],
                "",
                f"Business use case: {case['business_use_case']}",
                "",
                "| Layout | Block Mode | Median ms | Scanned Chunks | Scanned Blocks | Rows Scanned | Block Pruning Rate |",
                "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for layout in ["time", "symbol_time"]:
            for block_mode in ["coarse", "fine"]:
                result = case["layouts"][layout][block_mode]
                metrics = result["metrics"]
                runtime = result["runtime_ms"]["median"]
                lines.append(
                    f"| `{layout}` | `{block_mode}` | `{runtime}` | "
                    f"`{metrics['scanned_chunks']}` | `{metrics['scanned_blocks']}` | "
                    f"`{metrics['rows_scanned']}` | `{metrics['block_pruning_rate']:.4f}` |"
                )

            coarse = case["layouts"][layout]["coarse"]
            fine = case["layouts"][layout]["fine"]
            coarse_rows = coarse["metrics"]["rows_scanned"]
            fine_rows = fine["metrics"]["rows_scanned"]
            row_reduction = _fractional_reduction(coarse_rows, fine_rows)
            coarse_ms = coarse["runtime_ms"]["median"]
            fine_ms = fine["runtime_ms"]["median"]
            runtime_reduction = _fractional_reduction(coarse_ms, fine_ms)
            improvement_lines.append(
                f"- `{layout}` coarse -> fine: rows_scanned `{coarse_rows}` -> "
                f"`{fine_rows}` ({row_reduction:.2%} reduction), median_ms "
                f"`{coarse_ms}` -> `{fine_ms}` ({runtime_reduction:.2%} reduction)"
            )
        lines.append("")
        lines.extend(improvement_lines)
        lines.append("")

    return "\n".join(lines) + "\n"


def _render_console_summary(payload: dict[str, Any]) -> str:
    lines = ["Block index benchmark summary:"]
    for case in payload["cases"]:
        lines.append(f"- {case['title']}:")
        for layout in ["time", "symbol_time"]:
            coarse = case["layouts"][layout]["coarse"]
            fine = case["layouts"][layout]["fine"]
            coarse_metrics = coarse["metrics"]
            fine_metrics = fine["metrics"]
            lines.append(
                f"  {layout}: coarse_rows={coarse_metrics['rows_scanned']}, "
                f"fine_rows={fine_metrics['rows_scanned']}, "
                f"coarse_ms={coarse['runtime_ms']['median']}, "
                f"fine_ms={fine['runtime_ms']['median']}, "
                f"fine_block_pruning_rate={fine_metrics['block_pruning_rate']:.4f}"
            )
    return "\n".join(lines)


def _fractional_reduction(coarse_value: float, fine_value: float) -> float:
    if coarse_value <= 0:
        return 0.0
    return max(0.0, (coarse_value - fine_value) / coarse_value)


def _row_label(rows: int) -> str:
    if rows == 100_000:
        return "100k-rows"
    if rows == 1_000_000:
        return "1m-rows"
    return f"{rows}-rows"


if __name__ == "__main__":
    raise SystemExit(main())
