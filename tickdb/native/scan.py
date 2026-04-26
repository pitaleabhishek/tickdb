"""Load and call TickDB's tiny native numeric scan kernel.

The native layer evaluates one numeric predicate
over a block-local buffer and writes a one-byte-per-row match mask. Planning,
metadata pruning, aggregation, and correctness rechecks all stay in Python.
"""

from __future__ import annotations

import ctypes
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

DOUBLE_KIND = "double"
INT64_KIND = "int64"


@dataclass(frozen=True)
class NativePredicate:
    value_kind: str
    operator: str
    first_value: int | float
    second_value: int | float | None = None
    include_first: bool = True
    include_second: bool = True


_LIBRARY: ctypes.CDLL | None = None
_LOAD_ATTEMPTED = False


def native_scan_available() -> bool:
    return load_native_scan_library() is not None


def load_native_scan_library() -> ctypes.CDLL | None:
    global _LIBRARY, _LOAD_ATTEMPTED

    if _LOAD_ATTEMPTED:
        return _LIBRARY

    _LOAD_ATTEMPTED = True
    library_path = _native_library_path()
    if not _library_is_fresh(library_path):
        try:
            _build_native_library(library_path)
        except (FileNotFoundError, subprocess.CalledProcessError, OSError):
            _LIBRARY = None
            return None

    try:
        library = ctypes.CDLL(str(library_path))
    except OSError:
        # Rebuild once in case the on-disk library exists but is stale or
        # incompatible with the current platform/compiler output.
        try:
            _build_native_library(library_path)
            library = ctypes.CDLL(str(library_path))
        except (OSError, FileNotFoundError, subprocess.CalledProcessError):
            _LIBRARY = None
            return None

    _configure_signatures(library)
    _LIBRARY = library
    return _LIBRARY


def build_native_mask(
    raw_bytes: bytes,
    value_count: int,
    predicate: NativePredicate,
) -> bytes | None:
    library = load_native_scan_library()
    if library is None:
        return None
    if value_count < 0:
        raise ValueError("value_count must be non-negative")
    if value_count == 0:
        return b""

    element_width = 8
    expected_size = value_count * element_width
    if len(raw_bytes) != expected_size:
        raise ValueError(
            f"raw byte length {len(raw_bytes)} does not match expected size {expected_size}"
        )

    # ctypes needs a stable contiguous buffer; copying the block slice here
    # keeps the Python/C boundary small and predictable.
    input_buffer = (ctypes.c_char * len(raw_bytes)).from_buffer_copy(raw_bytes)
    mask = bytearray(value_count)
    # The C kernels write 0 for "no match" and 1 for "match" for each row.
    output_buffer = (ctypes.c_uint8 * value_count).from_buffer(mask)

    if predicate.value_kind == DOUBLE_KIND:
        values_ptr = ctypes.cast(input_buffer, ctypes.POINTER(ctypes.c_double))
        _call_double_kernel(
            library=library,
            values_ptr=values_ptr,
            value_count=value_count,
            predicate=predicate,
            output_buffer=output_buffer,
        )
        return bytes(mask)

    if predicate.value_kind == INT64_KIND:
        values_ptr = ctypes.cast(input_buffer, ctypes.POINTER(ctypes.c_int64))
        _call_int64_kernel(
            library=library,
            values_ptr=values_ptr,
            value_count=value_count,
            predicate=predicate,
            output_buffer=output_buffer,
        )
        return bytes(mask)

    raise ValueError(f"unsupported native predicate kind: {predicate.value_kind}")


def reset_native_scan_library() -> None:
    global _LIBRARY, _LOAD_ATTEMPTED
    _LIBRARY = None
    _LOAD_ATTEMPTED = False


def _call_double_kernel(
    library: ctypes.CDLL,
    values_ptr: ctypes.POINTER(ctypes.c_double),
    value_count: int,
    predicate: NativePredicate,
    output_buffer: ctypes.Array[ctypes.c_uint8],
) -> None:
    count = ctypes.c_size_t(value_count)
    if predicate.operator == ">":
        library.filter_gt_double(
            values_ptr,
            count,
            ctypes.c_double(float(predicate.first_value)),
            output_buffer,
        )
        return
    if predicate.operator == ">=":
        library.filter_ge_double(
            values_ptr,
            count,
            ctypes.c_double(float(predicate.first_value)),
            output_buffer,
        )
        return
    if predicate.operator == "<":
        library.filter_lt_double(
            values_ptr,
            count,
            ctypes.c_double(float(predicate.first_value)),
            output_buffer,
        )
        return
    if predicate.operator == "<=":
        library.filter_le_double(
            values_ptr,
            count,
            ctypes.c_double(float(predicate.first_value)),
            output_buffer,
        )
        return
    if predicate.operator == "between":
        if predicate.second_value is None:
            raise ValueError("between predicate requires second_value")
        library.filter_between_double(
            values_ptr,
            count,
            ctypes.c_double(float(predicate.first_value)),
            ctypes.c_uint8(1 if predicate.include_first else 0),
            ctypes.c_double(float(predicate.second_value)),
            ctypes.c_uint8(1 if predicate.include_second else 0),
            output_buffer,
        )
        return
    raise ValueError(f"unsupported double predicate operator: {predicate.operator}")


def _call_int64_kernel(
    library: ctypes.CDLL,
    values_ptr: ctypes.POINTER(ctypes.c_int64),
    value_count: int,
    predicate: NativePredicate,
    output_buffer: ctypes.Array[ctypes.c_uint8],
) -> None:
    count = ctypes.c_size_t(value_count)
    if predicate.operator == ">":
        library.filter_gt_int64(
            values_ptr,
            count,
            ctypes.c_int64(int(predicate.first_value)),
            output_buffer,
        )
        return
    if predicate.operator == ">=":
        library.filter_ge_int64(
            values_ptr,
            count,
            ctypes.c_int64(int(predicate.first_value)),
            output_buffer,
        )
        return
    if predicate.operator == "<":
        library.filter_lt_int64(
            values_ptr,
            count,
            ctypes.c_int64(int(predicate.first_value)),
            output_buffer,
        )
        return
    if predicate.operator == "<=":
        library.filter_le_int64(
            values_ptr,
            count,
            ctypes.c_int64(int(predicate.first_value)),
            output_buffer,
        )
        return
    if predicate.operator == "between":
        if predicate.second_value is None:
            raise ValueError("between predicate requires second_value")
        library.filter_between_int64(
            values_ptr,
            count,
            ctypes.c_int64(int(predicate.first_value)),
            ctypes.c_uint8(1 if predicate.include_first else 0),
            ctypes.c_int64(int(predicate.second_value)),
            ctypes.c_uint8(1 if predicate.include_second else 0),
            output_buffer,
        )
        return
    raise ValueError(f"unsupported int64 predicate operator: {predicate.operator}")


def _configure_signatures(library: ctypes.CDLL) -> None:
    double_ptr = ctypes.POINTER(ctypes.c_double)
    int64_ptr = ctypes.POINTER(ctypes.c_int64)
    uint8_ptr = ctypes.POINTER(ctypes.c_uint8)
    size_type = ctypes.c_size_t
    uint8_type = ctypes.c_uint8

    library.filter_gt_double.argtypes = [double_ptr, size_type, ctypes.c_double, uint8_ptr]
    library.filter_gt_double.restype = None
    library.filter_ge_double.argtypes = [double_ptr, size_type, ctypes.c_double, uint8_ptr]
    library.filter_ge_double.restype = None
    library.filter_lt_double.argtypes = [double_ptr, size_type, ctypes.c_double, uint8_ptr]
    library.filter_lt_double.restype = None
    library.filter_le_double.argtypes = [double_ptr, size_type, ctypes.c_double, uint8_ptr]
    library.filter_le_double.restype = None
    library.filter_between_double.argtypes = [
        double_ptr,
        size_type,
        ctypes.c_double,
        uint8_type,
        ctypes.c_double,
        uint8_type,
        uint8_ptr,
    ]
    library.filter_between_double.restype = None

    library.filter_gt_int64.argtypes = [int64_ptr, size_type, ctypes.c_int64, uint8_ptr]
    library.filter_gt_int64.restype = None
    library.filter_ge_int64.argtypes = [int64_ptr, size_type, ctypes.c_int64, uint8_ptr]
    library.filter_ge_int64.restype = None
    library.filter_lt_int64.argtypes = [int64_ptr, size_type, ctypes.c_int64, uint8_ptr]
    library.filter_lt_int64.restype = None
    library.filter_le_int64.argtypes = [int64_ptr, size_type, ctypes.c_int64, uint8_ptr]
    library.filter_le_int64.restype = None
    library.filter_between_int64.argtypes = [
        int64_ptr,
        size_type,
        ctypes.c_int64,
        uint8_type,
        ctypes.c_int64,
        uint8_type,
        uint8_ptr,
    ]
    library.filter_between_int64.restype = None


def _native_source_path() -> Path:
    return Path(__file__).with_name("scan.c")


def _native_library_path() -> Path:
    suffix = ".dylib" if sys.platform == "darwin" else ".so"
    return Path(__file__).with_name(f"libtickdb_scan{suffix}")


def _library_is_fresh(library_path: Path) -> bool:
    if not library_path.exists():
        return False
    return library_path.stat().st_mtime >= _native_source_path().stat().st_mtime


def _build_native_library(library_path: Path) -> None:
    source_path = _native_source_path()
    library_path.parent.mkdir(parents=True, exist_ok=True)

    if sys.platform == "darwin":
        # macOS wants a dynamic library; Linux uses the usual shared object.
        command = [
            "cc",
            "-O3",
            "-std=c99",
            "-dynamiclib",
            "-o",
            str(library_path),
            str(source_path),
        ]
    else:
        command = [
            "cc",
            "-O3",
            "-std=c99",
            "-shared",
            "-fPIC",
            "-o",
            str(library_path),
            str(source_path),
        ]

    subprocess.run(command, check=True, capture_output=True, text=True)
