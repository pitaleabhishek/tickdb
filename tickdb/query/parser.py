"""Parsing helpers for TickDB query planning."""

from __future__ import annotations

import re

from tickdb.query.models import (
    FLOAT_COLUMNS,
    GROUPABLE_COLUMNS,
    INTEGER_COLUMNS,
    NUMERIC_COLUMNS,
    SCHEMA_COLUMNS,
    AggregationSpec,
    FilterSpec,
    QuerySpec,
)

FILTER_PATTERN = re.compile(
    r"^(symbol|timestamp|open|high|low|close|volume)(<=|>=|=|<|>)(.+)$"
)
AGGREGATION_FUNCTIONS = {"count", "sum", "avg", "min", "max"}


def build_query_spec(
    table: str,
    aggregation_tokens: list[str],
    filter_tokens: list[str] | None = None,
    group_by_tokens: list[str] | None = None,
) -> QuerySpec:
    if not aggregation_tokens:
        raise ValueError("at least one --agg is required")

    filters = [parse_filter_token(token) for token in (filter_tokens or [])]
    aggregations = [parse_aggregation_token(token) for token in aggregation_tokens]
    group_by = _parse_group_by_tokens(group_by_tokens or [])

    return QuerySpec(
        table=table,
        filters=filters,
        aggregations=aggregations,
        group_by=group_by,
    )


def parse_filter_token(token: str) -> FilterSpec:
    match = FILTER_PATTERN.match(token.strip())
    if match is None:
        raise ValueError(
            f"invalid filter {token!r}; expected syntax like symbol=AAPL or close>100"
        )

    column, operator, raw_value = match.groups()
    value_text = raw_value.strip()
    if not value_text:
        raise ValueError(f"filter {token!r} is missing a value")

    if column == "symbol":
        if operator != "=":
            raise ValueError("symbol filters only support '='")
        return FilterSpec(column=column, operator=operator, value=value_text)

    if column in INTEGER_COLUMNS:
        value = int(value_text)
    elif column in FLOAT_COLUMNS:
        value = float(value_text)
    else:
        raise ValueError(f"unsupported filter column: {column}")

    return FilterSpec(column=column, operator=operator, value=value)


def parse_aggregation_token(token: str) -> AggregationSpec:
    normalized = token.strip()
    if normalized in {"count", "count:*"}:
        return AggregationSpec(function="count", column=None)

    if ":" not in normalized:
        raise ValueError(
            f"invalid aggregation {token!r}; expected count or func:column syntax"
        )

    function, column = normalized.split(":", 1)
    function = function.strip()
    column = column.strip()

    if function not in AGGREGATION_FUNCTIONS:
        raise ValueError(f"unsupported aggregation function: {function}")
    if function == "count":
        raise ValueError("count should be provided as 'count' or 'count:*'")
    if column not in SCHEMA_COLUMNS:
        raise ValueError(f"unsupported aggregation column: {column}")
    if column not in NUMERIC_COLUMNS:
        raise ValueError(f"aggregation column must be numeric: {column}")

    return AggregationSpec(function=function, column=column)


def _parse_group_by_tokens(tokens: list[str]) -> list[str]:
    group_by: list[str] = []
    for token in tokens:
        column = token.strip()
        if column not in GROUPABLE_COLUMNS:
            raise ValueError(f"unsupported group-by column: {column}")
        if column not in group_by:
            group_by.append(column)
    return group_by

