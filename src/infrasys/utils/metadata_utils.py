import sqlite3
from functools import singledispatch

from loguru import logger

from infrasys import TIME_SERIES_ASSOCIATIONS_TABLE
from infrasys.time_series_models import (
    DeterministicMetadata,
    SingleTimeSeriesMetadataBase,
    TimeSeriesMetadata,
)
from infrasys.utils.sqlite import execute
from infrasys.utils.time_utils import to_iso_8601


def create_associations_table(
    connection: sqlite3.Connection, table_name=TIME_SERIES_ASSOCIATIONS_TABLE
) -> bool:
    """Create the time series associations table schema on a DB connection.

    Parameters
    ----------
    connection: sqlite3.Connection
        SQLite connection to the metadata store database.
    table_name: str, default: 'time_series_associations'
        Name of the table to create.

    Returns
    -------
    bool
        True if the table was created succesfully.
    """
    schema = [
        "id INTEGER PRIMARY KEY",
        "time_series_uuid TEXT NOT NULL",
        "time_series_type TEXT NOT NULL",
        "initial_timestamp TEXT",
        "resolution TEXT NULL",
        "horizon TEXT",
        "interval TEXT",
        "window_count INTEGER",
        "length INTEGER",
        "name TEXT NOT NULL",
        "owner_uuid TEXT NOT NULL",
        "owner_type TEXT NOT NULL",
        "owner_category TEXT NOT NULL",
        "features TEXT NOT NULL",
        "scaling_factor_multiplier TEXT NULL",
        "metadata_uuid TEXT NOT NULL",
        "units TEXT NULL",
    ]
    schema_text = ",".join(schema)
    cur = connection.cursor()
    execute(cur, f"CREATE TABLE {table_name}({schema_text})")
    logger.debug("Created time series associations table")

    # Return true if the table creation was succesfull
    result = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
    ).fetchone()

    if not result:
        msg = "Could not create the associations table."
        raise RuntimeError(msg)

    connection.commit()
    return bool(result)


@singledispatch
def get_resolution(metadata: TimeSeriesMetadata) -> str | None:
    """Get formatted resolution from metadata or None if not available."""
    return None


@get_resolution.register
def _(metadata: SingleTimeSeriesMetadataBase) -> str:
    """Get resolution from SingleTimeSeriesMetadataBase."""
    return to_iso_8601(metadata.resolution)


@get_resolution.register
def _(metadata: DeterministicMetadata) -> str:
    """Get resolution from DeterministicMetadata."""
    return to_iso_8601(metadata.resolution)


@singledispatch
def get_initial_timestamp(metadata: TimeSeriesMetadata) -> str | None:
    """Get formatted initial_timestamp from metadata or None if not available."""
    return None


@get_initial_timestamp.register
def _(metadata: SingleTimeSeriesMetadataBase) -> str:
    """Get initial_timestamp from SingleTimeSeriesMetadataBase. Format for initial_timestamp is YYYY-MM-DDThh:mm:ss."""
    return metadata.initial_timestamp.isoformat(sep="T")


@get_initial_timestamp.register
def _(metadata: DeterministicMetadata) -> str:
    """Get initial_timestamp from DeterministicMetadata. Format for initial_timestamp is YYYY-MM-DDThh:mm:ss"""
    return metadata.initial_timestamp.isoformat(sep="T")


@singledispatch
def get_horizon(metadata: TimeSeriesMetadata) -> str | None:
    """Get formatted horizon from metadata or None if not available."""
    return None


@get_horizon.register
def _(metadata: DeterministicMetadata) -> str:
    """Get horizon from DeterministicMetadata."""
    return to_iso_8601(metadata.horizon)


@singledispatch
def get_interval(metadata: TimeSeriesMetadata) -> str | None:
    """Get formatted interval from metadata or None if not available."""
    return None


@get_interval.register
def _(metadata: DeterministicMetadata) -> str:
    """Get interval from DeterministicMetadata."""
    return to_iso_8601(metadata.interval)


@singledispatch
def get_window_count(metadata: TimeSeriesMetadata) -> int | None:
    """Get window_count from metadata or None if not available."""
    return None


@get_window_count.register
def _(metadata: DeterministicMetadata) -> int:
    """Get window_count from DeterministicMetadata."""
    return metadata.window_count


@singledispatch
def get_length(metadata: TimeSeriesMetadata) -> int | None:
    """Get length from metadata or None if not available."""
    return None


@get_length.register
def _(metadata: SingleTimeSeriesMetadataBase) -> int:
    """Get length from SingleTimeSeriesMetadataBase."""
    return metadata.length
