"""Row-level filter evaluation for query execution."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from tickdb.query.models import FilterSpec


def row_matches_filters(
    row_values: Mapping[str, Any],
    filters: list[FilterSpec],
) -> bool:
    return all(
        _value_matches_filter(row_values[filter_spec.column], filter_spec)
        for filter_spec in filters
    )


def _value_matches_filter(value: Any, filter_spec: FilterSpec) -> bool:
    target = filter_spec.value

    if filter_spec.operator == "=":
        return value == target
    if filter_spec.operator == ">":
        return value > target
    if filter_spec.operator == ">=":
        return value >= target
    if filter_spec.operator == "<":
        return value < target
    if filter_spec.operator == "<=":
        return value <= target

    raise ValueError(f"unsupported filter operator: {filter_spec.operator}")
