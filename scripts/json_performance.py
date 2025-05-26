"""
This script benchmarks the performance of loading and dumping JSON data
using the standard `json` library and the `orjson` library.

It can be run using `pytest`.

Usage:
    To run with a specific JSON data file from the project folder:
    ```terminal
    pytest scripts/json_performance.py --json-data path/to/your/data.json
    ```

    If `--json-data` is not provided, it will use a temporary example
    JSON file for benchmarking.

    To compare similar operations (e.g., dumps vs dumps or loads vs loads) run the following:
    ```bash
    pytest scripts/json_performance -k dump
    ```
    or
    ```bash
    pytest scripts/json_performance -k load
    ```
"""

import json
import pathlib

import pytest

orjson = pytest.importorskip("orjson", reason="orjson library not installed")
pytest.importorskip("pytest_benchmark", reason="pytest-benchmark not installed")


def load_with_standard_json(file_path: pathlib.Path):
    """Loads JSON using the standard json library."""
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data


def load_with_orjson(file_path: pathlib.Path):
    """Loads JSON using the orjson library."""
    with open(file_path, "rb") as f:
        data = orjson.loads(f.read())
    return data


def dump_with_standard_json(data, target_path: pathlib.Path):
    """Dumps data using the standard json library."""
    with open(target_path, "w", encoding="utf-8") as f:
        json.dump(data, f)


def dump_with_orjson(data, target_path: pathlib.Path):
    """Dumps data using the orjson library."""
    dumped_data = orjson.dumps(data)
    with open(target_path, "wb") as f:
        f.write(dumped_data)


@pytest.mark.parametrize(
    "load_func",
    [load_with_standard_json, load_with_orjson],
    ids=["standard_json_load", "orjson_load"],
)
def test_json_load_performance(benchmark, load_func, json_file_path):
    """Benchmark loading JSON from the specified file."""
    benchmark(load_func, json_file_path)


@pytest.mark.parametrize(
    "dump_func, lib_name",
    [
        (dump_with_standard_json, "standard_json"),
        (dump_with_orjson, "orjson"),
    ],
    ids=["standard_json_dump", "orjson_dump"],
)
def test_json_dump_performance(
    benchmark, dump_func, lib_name, json_file_path, tmp_path, json_data_from_file
):
    """Benchmark dumping JSON data to a temporary file."""
    output_file = tmp_path / f"output_{lib_name}.json"
    benchmark(dump_func, json_data_from_file, output_file)
