from __future__ import annotations

import struct
import unittest

from tickdb.native import (
    DOUBLE_KIND,
    INT64_KIND,
    NativePredicate,
    build_native_mask,
    load_native_scan_library,
)


class NativeScanTests(unittest.TestCase):
    def test_double_gt_mask(self) -> None:
        if load_native_scan_library() is None:
            self.skipTest("native scan library unavailable")

        raw = struct.pack("<4d", 10.5, 10.8, 20.5, 21.5)
        predicate = NativePredicate(
            value_kind=DOUBLE_KIND,
            operator=">",
            first_value=15.0,
        )

        mask = build_native_mask(raw, 4, predicate)

        self.assertEqual(mask, bytes([0, 0, 1, 1]))

    def test_int64_between_mask(self) -> None:
        if load_native_scan_library() is None:
            self.skipTest("native scan library unavailable")

        raw = struct.pack("<4q", 0, 10, 20, 30)
        predicate = NativePredicate(
            value_kind=INT64_KIND,
            operator="between",
            first_value=10,
            second_value=30,
            include_first=True,
            include_second=False,
        )

        mask = build_native_mask(raw, 4, predicate)

        self.assertEqual(mask, bytes([0, 1, 1, 0]))
