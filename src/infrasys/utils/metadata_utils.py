import sqlite3

from loguru import logger

from infrasys import TIME_SERIES_ASSOCIATIONS_TABLE
from infrasys.utils.sqlite import execute


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
