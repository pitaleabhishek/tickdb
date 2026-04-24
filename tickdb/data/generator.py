"""Synthetic OHLCV data generation."""

from __future__ import annotations

import csv
import random
from pathlib import Path

FIELDNAMES = ["symbol", "timestamp", "open", "high", "low", "close", "volume"]


def generate_csv(
    output_path: Path,
    symbols: list[str],
    rows: int,
    start_timestamp: int,
    step_seconds: int,
    seed: int,
) -> int:
    """Generate deterministic synthetic OHLCV rows and write them to CSV."""
    if not symbols:
        raise ValueError("at least one symbol is required")
    if rows <= 0:
        raise ValueError("rows must be positive")
    if step_seconds <= 0:
        raise ValueError("step-seconds must be positive")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    random.seed(seed)
    last_close_by_symbol = {
        symbol: 100.0 + (index * 25.0) for index, symbol in enumerate(symbols)
    }

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()

        for row_index in range(rows):
            symbol = symbols[row_index % len(symbols)]
            timestamp = start_timestamp + (row_index * step_seconds)
            previous_close = last_close_by_symbol[symbol]

            drift = random.uniform(-1.25, 1.25)
            open_price = previous_close
            close_price = max(0.01, open_price + drift)
            high_price = max(open_price, close_price) + random.uniform(0.0, 0.75)
            low_price = min(open_price, close_price) - random.uniform(0.0, 0.75)
            volume = random.randint(50_000, 5_000_000)

            last_close_by_symbol[symbol] = close_price

            writer.writerow(
                {
                    "symbol": symbol,
                    "timestamp": timestamp,
                    "open": f"{open_price:.4f}",
                    "high": f"{high_price:.4f}",
                    "low": f"{low_price:.4f}",
                    "close": f"{close_price:.4f}",
                    "volume": volume,
                }
            )

    return rows

