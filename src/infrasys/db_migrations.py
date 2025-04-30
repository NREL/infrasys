import json
import sqlite3
import uuid

from loguru import logger

from infrasys import (
    KEY_VALUE_STORE_TABLE,
    TIME_SERIES_ASSOCIATIONS_TABLE,
    TIME_SERIES_METADATA_TABLE,
)
from infrasys.serialization import TYPE_METADATA
from infrasys.time_series_metadata_store import make_features_string
from infrasys.utils.metadata_utils import create_associations_table
from infrasys.utils.sqlite import execute
from infrasys.utils.time_utils import str_timedelta_to_iso_8601

_LEGACY_METADATA_TABLE = "legacy_metadata_backup"


def metadata_needs_migration(conn: sqlite3.Connection, version: str | None = None) -> bool:
    """Check if the database schema requires migration to the new format.

    Parameters
    ----------
    conn : sqlite3.Connection
        An active SQLite database connection.

    Returns
    -------
    bool
        True if migration is required (new table does not exist), False otherwise.
    """
    cursor = conn.cursor()
    query = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1"
    cursor.execute(query, (TIME_SERIES_ASSOCIATIONS_TABLE,))
    return not cursor.fetchone() is not None


def migrate_legacy_schema(conn: sqlite3.Connection) -> bool:
    """Migrate the database from the legacy schema to the new separated schema.

    Handles the transition from an older schema (where time series metadata and
    associations were likely combined) to a newer schema featuring separate
    `TIME_SERIES_ASSOCIATIONS_TABLE` and `KEY_VALUE_STORE_TABLE`.

    Parameters
    ----------
    conn : sqlite3.Connection
        An active SQLite database connection where the migration will be performed.

    Returns
    -------
    bool
        True if the migration was performed successfully.

    Notes
    -----
    The migration process involves these steps:
    1. Verify the existing `TIME_SERIES_METADATA_TABLE` matches the expected
       legacy column structure.
    2. Rename the legacy table to a temporary backup name.
    3. Create the new `KEY_VALUE_STORE_TABLE` and `TIME_SERIES_ASSOCIATIONS_TABLE`.
    4. Read data row-by-row from the backup table.
    5. Transform legacy data:
       - Extract `user_attributes` from `metadata` JSON, renaming to `features`.
       - Convert string timedelta `resolution` to ISO 8601 duration format.
       - Set default `owner_category` to "Component".
       - Set default empty JSON object for `serialization_info`.
    6. Insert transformed data into the new `TIME_SERIES_ASSOCIATIONS_TABLE`.
    7. Create required indexes on the new associations table.
    8. Drop the temporary backup table.
    9. Commit the transaction.

    Returns
    -------
    bool:
        True if migration was successful
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

    logger.debug("Creating backup tables.")
    execute(
        cursor,
        f"ALTER TABLE {TIME_SERIES_METADATA_TABLE} RENAME TO {_LEGACY_METADATA_TABLE}",
    )

    logger.info("Creating new schema tables.")
    execute(
        cursor, f"CREATE TABLE {KEY_VALUE_STORE_TABLE}(key TEXT PRIMARY KEY, VALUE JSON NOT NULL)"
    )
    create_associations_table(connection=conn)

    logger.info("Migrating data from legacy schema.")
    cursor.execute(f"SELECT * FROM {_LEGACY_METADATA_TABLE}")
    rows = cursor.fetchall()

    sql_data_to_insert = []
    for row in rows:
        (
            id_val,
            time_series_uuid,
            time_series_type,
            initial_timestamp,
            resolution,
            name,
            owner_uuid,
            owner_type,
            features_hash,
            metadata_json,
        ) = row

        metadata = json.loads(metadata_json)

        # Creating a flatten metadata from legacy schema.
        serialized_info = {
            "__metadata__": {
                **{
                    "quantity_metadata": metadata.get("quantity_metadata"),
                    "normalization": metadata.get("normalization"),
                },
                **metadata[TYPE_METADATA].pop("fields", {}),
            }
        }

        features_dict = {}
        if metadata.get("user_attributes"):  # We renamed user_attributes to features
            features_dict = metadata.pop("user_attributes")

        owner_category = "Component"
        length = metadata.get("length", 0)

        # Old resolution was in timedelta format.
        resolution = str_timedelta_to_iso_8601(resolution)

        # Fix for timestamp from: 2020-01-01 00:00 -> 2020-01-01T00:00
        initial_timestamp = initial_timestamp.replace(" ", "T")
        sql_data_to_insert.append(
            {
                "time_series_uuid": time_series_uuid,
                "time_series_type": time_series_type,
                "initial_timestamp": initial_timestamp,
                "resolution": resolution,
                "length": length,
                "name": name,
                "owner_uuid": owner_uuid,
                "owner_type": owner_type,
                "owner_category": owner_category,
                "features_json": make_features_string(features_dict),
                "serialization_info": json.dumps(serialized_info),
                "uuid": str(uuid.uuid4()),  # metadata_uuid did not exist on tehe legacy
            }
        )
    # Exit if there is no data to ingest.
    if not sql_data_to_insert:
        execute(cursor, f"DROP TABLE {_LEGACY_METADATA_TABLE}")
        conn.commit()
        logger.info("Schema migration completed.")
        return True

    # If we do have data, we insert it
    logger.info(
        f"Inserting {len(sql_data_to_insert)} records into {TIME_SERIES_ASSOCIATIONS_TABLE}."
    )
    cursor.executemany(
        f"""
        INSERT INTO `{TIME_SERIES_ASSOCIATIONS_TABLE}` (
            time_series_uuid, time_series_type, initial_timestamp, resolution,
            length, name, owner_uuid, owner_type, owner_category, features,
            serialization_info, uuid
        ) VALUES (
            :time_series_uuid, :time_series_type, :initial_timestamp, :resolution,
            :length, :name, :owner_uuid, :owner_type, :owner_category,
            :features_json, :serialization_info, :uuid
        )
        """,
        sql_data_to_insert,
    )

    logger.info("Creating indexes on {}.", TIME_SERIES_ASSOCIATIONS_TABLE)
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

    # Dropping legacy table since it is no longer required.
    execute(cursor, f"DROP TABLE {_LEGACY_METADATA_TABLE}")
    conn.commit()
    logger.info("Schema migration completed.")
    return True
