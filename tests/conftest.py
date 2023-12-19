import os
import sys
from datetime import datetime, timedelta

import polars as pl
import pytest

from infra_sys.time_series_models import TIME_COLUMN, VALUE_COLUMN

sys.path.append(os.path.join(os.path.dirname(__file__), "models"))


@pytest.fixture
def hourly_time_array() -> pl.DataFrame:
    """Provides a DataFrame with hourly data for a year."""
    start = datetime(year=2021, month=1, day=1)
    end = datetime(year=2021, month=12, day=31, hour=23)
    resolution = timedelta(hours=1)
    length = 8760
    return pl.DataFrame(
        {
            TIME_COLUMN: pl.datetime_range(start, end, interval=resolution, eager=True),
            VALUE_COLUMN: range(length),
        },
    )
