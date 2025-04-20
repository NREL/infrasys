import json
import pathlib

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--json-data",
        action="store",
        type=str,
        default=None,
        help="Path to the JSON data file for both load and dump benchmarks",
    )


@pytest.fixture
def json_file_path(request, tmp_path):
    file_path_str = request.config.getoption("--json-data")
    if file_path_str:
        path = pathlib.Path(file_path_str)
        if not path.exists():
            pytest.fail(f"JSON data file not found at: {path}")
        return path
    else:
        # Create a temporary JSON file with example data if no --json-data is provided
        example_data = {"name": "example", "value": 123, "items": [1, 2, 3, {"nested": True}]}
        temp_file = tmp_path / "example_data.json"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(example_data, f)
        print(f"Using example JSON data from: {temp_file} for both load and dump benchmarks")
        return temp_file


@pytest.fixture
def json_data_from_file(json_file_path):
    """Fixture to load data from the json_file_path for dumping benchmarks."""
    try:
        with open(json_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except Exception as e:
        pytest.fail(f"Error loading data from {json_file_path}: {e}")
        return None


@pytest.fixture
def json_data():
    """Fixture to provide sample JSON data for dumping tests (if needed independently)."""
    return {"name": "example", "value": 123, "items": [1, 2, 3, {"nested": True}]}
