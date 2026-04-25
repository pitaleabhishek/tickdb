"""Metadata-level pruning helpers."""

from __future__ import annotations

from typing import Any

from tickdb.query.models import FilterSpec


def metadata_matches_filters(metadata: Any, filters: list[FilterSpec]) -> bool:
    return all(_metadata_matches_filter(metadata, filter_spec) for filter_spec in filters)


def _metadata_matches_filter(metadata: Any, filter_spec: FilterSpec) -> bool:
    if filter_spec.column == "symbol":
        return str(filter_spec.value) in metadata.symbols

    minimum = _metric_value(metadata, f"{filter_spec.column}_min")
    maximum = _metric_value(metadata, f"{filter_spec.column}_max")
    value = filter_spec.value

    if filter_spec.operator == "=":
        return minimum <= value <= maximum
    if filter_spec.operator == ">":
        return maximum > value
    if filter_spec.operator == ">=":
        return maximum >= value
    if filter_spec.operator == "<":
        return minimum < value
    if filter_spec.operator == "<=":
        return minimum <= value

    raise ValueError(f"unsupported filter operator: {filter_spec.operator}")


def _metric_value(metadata: Any, field_name: str) -> int | float:
    if not hasattr(metadata, field_name):
        raise ValueError(f"metadata object does not support field {field_name}")
    return getattr(metadata, field_name)
