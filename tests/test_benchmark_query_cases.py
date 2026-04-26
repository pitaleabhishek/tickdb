from __future__ import annotations

import unittest

from benchmarks.query_cases import (
    BenchmarkContext,
    build_benchmark_cases,
    build_block_index_cases,
    build_native_scan_cases,
)


class BenchmarkQueryCaseTests(unittest.TestCase):
    def test_build_benchmark_cases_returns_expected_shapes(self) -> None:
        context = BenchmarkContext(min_timestamp=1_000, max_timestamp=101_000)
        cases = build_benchmark_cases(context)

        self.assertEqual(
            [case.name for case in cases],
            [
                "full_scan_count",
                "time_window_avg_close",
                "symbol_volume_sum",
                "symbol_time_avg_close",
            ],
        )
        time_case = cases[1]
        self.assertEqual(time_case.aggregation_tokens, ["avg:close"])
        self.assertEqual(len(time_case.filter_tokens), 2)
        self.assertTrue(time_case.filter_tokens[0].startswith("timestamp>="))
        self.assertTrue(time_case.filter_tokens[1].startswith("timestamp<="))

        symbol_time_case = cases[3]
        self.assertIn("symbol=NVDA", symbol_time_case.filter_tokens)

    def test_window_respects_bounds(self) -> None:
        context = BenchmarkContext(min_timestamp=100, max_timestamp=200)
        start, end = context.window(start_fraction=0.9, width_fraction=0.5)

        self.assertGreaterEqual(start, 100)
        self.assertLessEqual(end, 200)
        self.assertLessEqual(start, end)

    def test_build_block_index_cases_returns_expected_shapes(self) -> None:
        context = BenchmarkContext(min_timestamp=1_000, max_timestamp=101_000)
        cases = build_block_index_cases(context)

        self.assertEqual(
            [case.name for case in cases],
            [
                "narrow_time_window_avg_close",
                "narrow_symbol_time_avg_close",
            ],
        )

        narrow_time_case = cases[0]
        self.assertEqual(narrow_time_case.aggregation_tokens, ["avg:close"])
        self.assertEqual(len(narrow_time_case.filter_tokens), 2)
        self.assertTrue(narrow_time_case.filter_tokens[0].startswith("timestamp>="))
        self.assertTrue(narrow_time_case.filter_tokens[1].startswith("timestamp<="))

        narrow_symbol_time_case = cases[1]
        self.assertIn("symbol=NVDA", narrow_symbol_time_case.filter_tokens)

    def test_build_native_scan_cases_returns_expected_shapes(self) -> None:
        context = BenchmarkContext(min_timestamp=1_000, max_timestamp=101_000)
        cases = build_native_scan_cases(context)

        self.assertEqual(
            [case.name for case in cases],
            [
                "narrow_time_window_avg_close",
                "symbol_close_threshold_sum_volume",
            ],
        )

        time_case = cases[0]
        self.assertEqual(time_case.aggregation_tokens, ["avg:close"])
        self.assertEqual(len(time_case.filter_tokens), 2)
        self.assertTrue(time_case.filter_tokens[0].startswith("timestamp>="))
        self.assertTrue(time_case.filter_tokens[1].startswith("timestamp<="))

        close_case = cases[1]
        self.assertIn("symbol=NVDA", close_case.filter_tokens)
        self.assertIn("close>150", close_case.filter_tokens)
