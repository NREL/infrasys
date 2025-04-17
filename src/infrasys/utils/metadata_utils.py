import sqlite3

from loguru import logger

from infrasys import TIME_SERIES_ASSOCIATIONS_TABLE
from infrasys.utils.sqlite import execute


def create_associations_table(
    connection: sqlite3.Connection, table_name=TIME_SERIES_ASSOCIATIONS_TABLE
) -> bool:
    schema = [
        "id INTEGER PRIMARY KEY",
        "time_series_uuid TEXT NOT NULL",
        "time_series_type TEXT NOT NULL",
        "time_series_category TEXT NOT NULL",
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
        "metadata_uuid TEXT NOT NULL",
    ]
    schema_text = ",".join(schema)
    cur = connection.cursor()
    execute(cur, f"CREATE TABLE {TIME_SERIES_ASSOCIATIONS_TABLE}({schema_text})")
    logger.debug("Created time series associations table")

    # Return true if the table creation was succesfull
    result = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
    ).fetchone()
    connection.commit()
    return bool(result)
