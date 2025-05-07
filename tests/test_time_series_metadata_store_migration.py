import pytest

from infrasys import TIME_SERIES_METADATA_TABLE
from infrasys.migrations.db_migrations import (
    metadata_store_needs_migration,
    migrate_legacy_metadata_store,
)
from infrasys.time_series_metadata_store import TimeSeriesMetadataStore
from infrasys.utils.sqlite import create_in_memory_db, execute

from .models.simple_system import SimpleSystem


@pytest.fixture
def legacy_system(pytestconfig):
    return pytestconfig.rootpath.joinpath("tests/data/legacy_system.json")


@pytest.fixture(scope="function")
def legacy_db():
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
    conn = create_in_memory_db()
    schema_text = ",".join(legacy_columns)
    cur = conn.cursor()
    execute(cur, f"CREATE TABLE {TIME_SERIES_METADATA_TABLE}({schema_text})")
    old_schema_data = (
        1,
        "33d47754-ff74-44d8-b279-2eac914d1d5e",
        "SingleTimeSeries",
        "2020-01-01 00:00:00",
        "1:00:00",
        "active_power",
        "d65fa5b9-a735-4b79-b880-27a5058c533e",
        "SimpleGenerator",
        None,
        '{"variable_name": "active_power", "initial_time": "2020-01-01T00:00:00", "resolution": "PT1H", "time_series_uuid": "33d47754-ff74-44d8-b279-2eac914d1d5e", "user_attributes": {}, "quantity_metadata": {"module": "infrasys.quantities", "quantity_type": "ActivePower", "units": "watt"}, "normalization": {"test":true}, "type": "SingleTimeSeries", "length": 10, "__metadata__": {"fields": {"module": "infrasys.time_series_models", "type": "SingleTimeSeriesMetadata", "serialized_type": "base"}}}',
    )
    placeholders = ", ".join("?" * len(old_schema_data))
    breakpoint()
    execute(cur, f"INSERT INTO {TIME_SERIES_METADATA_TABLE}({placeholders})", old_schema_data)
    conn.commit()
    yield conn
    conn.close()


def test_metadata_version_detection():
    conn = create_in_memory_db()
    metadata_store = TimeSeriesMetadataStore(conn, initialize=True)

    assert isinstance(metadata_store, TimeSeriesMetadataStore)
    assert not metadata_store_needs_migration(conn)


def test_migrate_old_system(legacy_system):
    system = SimpleSystem.from_json(legacy_system)
    conn = system._time_series_mgr._metadata_store._con
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    assert "time_series_associations" in tables


def test_migrate_without_columns(legacy_system):
    conn = create_in_memory_db()
    conn.execute(f"CREATE TABlE {TIME_SERIES_METADATA_TABLE}(id, test)")
    with pytest.raises(NotImplementedError):
        migrate_legacy_metadata_store(conn)


def test_migrating_schema_with_no_entires(caplog):
    legacy_columns = [
        "id",
        "time_series_uuid",
        "time_series_type",
        "initial_time",
        "resolution",
        "variable_name",
        "component_uuid",
        "component_type",
        "normalization",
        "user_attributes_hash",
        "metadata",
    ]
    conn = create_in_memory_db()
    schema_text = ",".join(legacy_columns)
    cur = conn.cursor()
    execute(cur, f"CREATE TABLE {TIME_SERIES_METADATA_TABLE}({schema_text})")
    conn.commit()
    assert migrate_legacy_metadata_store(conn)
