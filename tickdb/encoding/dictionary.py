"""Dictionary encoding helpers for string columns."""

from __future__ import annotations

import json
import struct
from pathlib import Path
from typing import Sequence


def write_dictionary_files(
    dictionary_path: Path,
    ids_path: Path,
    values: Sequence[str],
) -> tuple[list[str], list[int]]:
    dictionary_values, encoded_ids = encode_dictionary(values)
    payload = {
        "encoding": "dictionary",
        "values": dictionary_values,
    }
    dictionary_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    ids_path.write_bytes(_pack_uint32(encoded_ids))
    return dictionary_values, encoded_ids


def encode_dictionary(values: Sequence[str]) -> tuple[list[str], list[int]]:
    dictionary_values: list[str] = []
    encoded_ids: list[int] = []
    ids_by_value: dict[str, int] = {}

    for value in values:
        if value not in ids_by_value:
            ids_by_value[value] = len(dictionary_values)
            dictionary_values.append(value)
        encoded_ids.append(ids_by_value[value])

    return dictionary_values, encoded_ids


def read_dictionary_file(path: Path) -> list[str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [str(value) for value in payload["values"]]


def read_uint32_file(path: Path) -> list[int]:
    data = path.read_bytes()
    if not data:
        return []
    value_width = struct.calcsize("<I")
    if len(data) % value_width != 0:
        raise ValueError("dictionary id file has invalid size")
    count = len(data) // value_width
    return [int(value) for value in struct.unpack(f"<{count}I", data)]


def decode_dictionary_values(dictionary_path: Path, ids_path: Path) -> list[str]:
    dictionary_values = read_dictionary_file(dictionary_path)
    encoded_ids = read_uint32_file(ids_path)
    return [dictionary_values[index] for index in encoded_ids]


def _pack_uint32(values: Sequence[int]) -> bytes:
    if not values:
        return b""
    return struct.pack(f"<{len(values)}I", *values)

