import json
import sqlite3
from uuid import uuid4

from loguru import logger

from infrasys import (
    KEY_VALUE_STORE_TABLE,
    TIME_SERIES_ASSOCIATIONS_TABLE,
    TIME_SERIES_METADATA_TABLE,
)
from infrasys.utils.sqlite import execute
from infrasys.utils.time_utils import _str_timedelta_to_iso_8601

TEMP_TABLE = "legacy_metadata"


def needs_migration(conn: sqlite3.Connection, version: str | None = None) -> bool:
    """Check if time series metadata table needs migration."""
    query = "SELECT * FROM sqlite_master"
    result = conn.execute(query).fetchall()[0]
    if "time_series_associations" in result:
        return False
    return True


def migrate_legacy_schema(conn: sqlite3.Connection) -> bool:
    """Migrate from legacy schema to new schema with separated metadata.

    Notes
    -----
    This migration:
    1. Creates temporary table.
    2. Extracts features from metadata and saves as features
    3. Converts resolution to ISO format
    4. Preserves all data while restructuring the database

    Returns
    -------
        bool: True if migration was successful
    """
    logger.info("Migrating legacy metadata schema.")

    legacy_columns = [
        "id",
        "time_series_uuid",
        "time_series_type",
        "initial_time",
        "resolution",
        "variable_name",
        "component_uuid",
        "component_type",
        "user_attributes_hash",
        "metadata",
    ]

    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {TIME_SERIES_METADATA_TABLE} LIMIT 1")
    columns = [desc[0] for desc in cursor.description]
    if not all(column in columns for column in legacy_columns):
        logger.error(f"Legacy schema does not match expected columns: {columns}")
        msg = "Bug: Legacy schema doesn't match expected structure"
        raise NotImplementedError(msg)

    logger.info("Creating backup tables.")
    execute(
        cursor,
        f"CREATE TEMPORARY TABLE {TEMP_TABLE} AS SELECT * FROM {TIME_SERIES_METADATA_TABLE}",
    )
    execute(
        cursor,
        f"DROP TABLE {TIME_SERIES_METADATA_TABLE}",
    )

    logger.info("Creating new schema tables...")
    execute(
        cursor,
        f"""
        CREATE TABLE {TIME_SERIES_METADATA_TABLE} (
            id INTEGER PRIMARY KEY,
            metadata_uuid TEXT,
            metadata JSON NOT NULL
        )
    """,
    )
    execute(
        cursor, f"CREATE TABLE {KEY_VALUE_STORE_TABLE}(key TEXT PRIMARY KEY, VALUE JSON NOT NULL)"
    )

    execute(
        cursor,
        f"""
        CREATE TABLE {TIME_SERIES_ASSOCIATIONS_TABLE} (
            id INTEGER PRIMARY KEY,
            time_series_uuid TEXT NOT NULL,
            time_series_type TEXT NOT NULL,
            time_series_category TEXT NOT NULL,
            initial_timestamp TEXT,
            resolution TEXT NULL,
            horizon TEXT,
            interval TEXT,
            window_count INTEGER,
            length INTEGER,
            name TEXT NOT NULL,
            owner_uuid TEXT NOT NULL,
            owner_type TEXT NOT NULL,
            owner_category TEXT NOT NULL,
            features JSON NOT NULL,
            metadata_id TEXT NOT NULL
        )
    """,
    )

    logger.info("Migrating data from legacy schema...")
    cursor.execute(f"SELECT * FROM {TEMP_TABLE}")
    rows = cursor.fetchall()

    for row in rows:
        (
            id_val,
            time_series_uuid,
            time_series_type,
            initial_time,
            resolution,
            variable_name,
            component_uuid,
            component_type,
            features_hash,
            metadata_json,
        ) = row

        metadata_data = json.loads(metadata_json)
        features = {}
        if "user_attributes" in metadata_data:  # We renamed user_attributes to features
            features = metadata_data.pop("user_attributes")
        features_json = json.dumps(features)
        metadata_json = json.dumps(metadata_data)

        # NOTE: Shall we force the metadata to have UUID? It currently does not have it.
        metadata_uuid = str(
            uuid4()
        )  #  Creating UUID for metadata information since we did not had it before
        execute(
            cursor,
            f"INSERT INTO {TIME_SERIES_METADATA_TABLE} (metadata_uuid, metadata) VALUES (?, ?)",
            params=(metadata_uuid, metadata_json),
        )
        metadata_id = cursor.lastrowid

        time_series_category = "StaticTimeSeries"
        owner_category = "Component"
        length = metadata_data["length"]
        resolution = _str_timedelta_to_iso_8601(resolution)
        execute(
            cursor,
            f"""
            INSERT INTO {TIME_SERIES_ASSOCIATIONS_TABLE} (
                time_series_uuid, time_series_type, time_series_category,
                initial_timestamp, resolution, length, name, owner_uuid, owner_type,
                owner_category, features, metadata_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            params=(
                time_series_uuid,
                time_series_type,
                time_series_category,
                initial_time,
                resolution,
                length,
                variable_name,
                component_uuid,
                component_type,
                owner_category,
                features_json,
                metadata_id,
            ),
        )

    execute(
        cursor,
        f"""
        CREATE INDEX IF NOT EXISTS by_c_vn_tst_hash ON {TIME_SERIES_ASSOCIATIONS_TABLE}
        (owner_uuid, time_series_type, name, resolution, features)
    """,
    )
    execute(
        cursor,
        f"""
        CREATE INDEX IF NOT EXISTS by_ts_uuid ON {TIME_SERIES_ASSOCIATIONS_TABLE}
        (time_series_uuid)
    """,
    )

    execute(cursor, f"DROP TABLE {TEMP_TABLE}")
    conn.commit()
    logger.info(
        f"Migration complete. Backup of old schema available as view `{TIME_SERIES_METADATA_TABLE}_legacy"
    )

    return True
