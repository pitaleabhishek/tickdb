"""Fixed benchmark query cases for layout comparisons."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BenchmarkContext:
    min_timestamp: int
    max_timestamp: int

    @property
    def span(self) -> int:
        return max(1, self.max_timestamp - self.min_timestamp)

    def window(self, start_fraction: float, width_fraction: float) -> tuple[int, int]:
        start = self.min_timestamp + int(self.span * start_fraction)
        end = start + max(1, int(self.span * width_fraction))
        return start, min(end, self.max_timestamp)


@dataclass(frozen=True)
class BenchmarkCase:
    name: str
    description: str
    business_use_case: str
    aggregation_tokens: list[str]
    filter_tokens: list[str]
    group_by_tokens: list[str]


def build_benchmark_cases(context: BenchmarkContext) -> list[BenchmarkCase]:
    time_start, time_end = context.window(start_fraction=0.45, width_fraction=0.01)
    symbol_time_start, symbol_time_end = context.window(
        start_fraction=0.40,
        width_fraction=0.05,
    )

    return [
        BenchmarkCase(
            name="full_scan_count",
            description="Full table count baseline with no pruning.",
            business_use_case=(
                "Baseline scan cost for table-wide health checks and sanity counts."
            ),
            aggregation_tokens=["count"],
            filter_tokens=[],
            group_by_tokens=[],
        ),
        BenchmarkCase(
            name="time_window_avg_close",
            description="Average close over a narrow mid-stream time window.",
            business_use_case=(
                "Measure market state over a recent window without targeting a symbol."
            ),
            aggregation_tokens=["avg:close"],
            filter_tokens=[
                f"timestamp>={time_start}",
                f"timestamp<={time_end}",
            ],
            group_by_tokens=[],
        ),
        BenchmarkCase(
            name="symbol_volume_sum",
            description="Volume sum for a single symbol across the full dataset.",
            business_use_case=(
                "Evaluate single-name liquidity without constraining the time range."
            ),
            aggregation_tokens=["sum:volume"],
            filter_tokens=["symbol=NVDA"],
            group_by_tokens=[],
        ),
        BenchmarkCase(
            name="symbol_time_avg_close",
            description="Average close for one symbol inside a wider time window.",
            business_use_case=(
                "Typical market-data query for one name during a targeted period."
            ),
            aggregation_tokens=["avg:close"],
            filter_tokens=[
                "symbol=NVDA",
                f"timestamp>={symbol_time_start}",
                f"timestamp<={symbol_time_end}",
            ],
            group_by_tokens=[],
        ),
    ]


def build_block_index_cases(context: BenchmarkContext) -> list[BenchmarkCase]:
    narrow_time_start, narrow_time_end = context.window(
        start_fraction=0.45,
        width_fraction=0.001,
    )
    narrow_symbol_time_start, narrow_symbol_time_end = context.window(
        start_fraction=0.40,
        width_fraction=0.001,
    )

    return [
        BenchmarkCase(
            name="narrow_time_window_avg_close",
            description=(
                "Average close over a narrow time window small enough to fit inside "
                "one chunk but much smaller than the chunk itself."
            ),
            business_use_case=(
                "Measure a short market interval where chunk pruning alone is too "
                "coarse and intra-chunk skipping should reduce scan work."
            ),
            aggregation_tokens=["avg:close"],
            filter_tokens=[
                f"timestamp>={narrow_time_start}",
                f"timestamp<={narrow_time_end}",
            ],
            group_by_tokens=[],
        ),
        BenchmarkCase(
            name="narrow_symbol_time_avg_close",
            description=(
                "Average close for one symbol inside a very narrow time window."
            ),
            business_use_case=(
                "Typical single-name OHLCV query where chunk pruning narrows the "
                "candidate set and block pruning should reduce the remaining scan."
            ),
            aggregation_tokens=["avg:close"],
            filter_tokens=[
                "symbol=NVDA",
                f"timestamp>={narrow_symbol_time_start}",
                f"timestamp<={narrow_symbol_time_end}",
            ],
            group_by_tokens=[],
        ),
    ]


def build_native_scan_cases(context: BenchmarkContext) -> list[BenchmarkCase]:
    time_start, time_end = context.window(
        start_fraction=0.45,
        width_fraction=0.001,
    )

    return [
        BenchmarkCase(
            name="narrow_time_window_avg_close",
            description=(
                "Average close over a narrow timestamp window to exercise the "
                "native int64 timestamp predicate path."
            ),
            business_use_case=(
                "Measure a short market interval where pruning already narrowed "
                "the scan and the remaining timestamp comparisons are the hot loop."
            ),
            aggregation_tokens=["avg:close"],
            filter_tokens=[
                f"timestamp>={time_start}",
                f"timestamp<={time_end}",
            ],
            group_by_tokens=[],
        ),
        BenchmarkCase(
            name="symbol_close_threshold_sum_volume",
            description=(
                "Volume sum for NVDA above a close threshold to exercise the "
                "native float64 price predicate path."
            ),
            business_use_case=(
                "Typical single-name threshold query where symbol filtering stays "
                "in Python but the price predicate is a good native pushdown candidate."
            ),
            aggregation_tokens=["sum:volume"],
            filter_tokens=[
                "symbol=NVDA",
                "close>150",
            ],
            group_by_tokens=[],
        ),
    ]
