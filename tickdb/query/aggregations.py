"""Aggregation state helpers for query execution."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from tickdb.query.models import AggregationSpec


@dataclass
class AggregationState:
    spec: AggregationSpec
    count: int = 0
    total: int | float = 0
    current: int | float | None = None


def initialize_aggregation_states(
    aggregations: list[AggregationSpec],
) -> list[AggregationState]:
    return [AggregationState(spec=aggregation) for aggregation in aggregations]


def update_aggregation_states(
    states: list[AggregationState],
    row_values: Mapping[str, Any],
) -> None:
    for state in states:
        function = state.spec.function
        if function == "count":
            state.count += 1
            continue

        assert state.spec.column is not None
        value = row_values[state.spec.column]
        if function == "sum":
            state.total += value
        elif function == "avg":
            state.total += value
            state.count += 1
        elif function == "min":
            if state.current is None or value < state.current:
                state.current = value
        elif function == "max":
            if state.current is None or value > state.current:
                state.current = value
        else:
            raise ValueError(f"unsupported aggregation function: {function}")


def finalize_aggregation_states(
    states: list[AggregationState],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for state in states:
        function = state.spec.function
        if function == "count":
            result[state.spec.result_key] = state.count
        elif function == "sum":
            result[state.spec.result_key] = state.total
        elif function == "avg":
            result[state.spec.result_key] = (
                state.total / state.count if state.count else None
            )
        elif function in {"min", "max"}:
            result[state.spec.result_key] = state.current
        else:
            raise ValueError(f"unsupported aggregation function: {function}")

    return result
