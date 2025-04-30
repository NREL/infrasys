import pytest

from infrasys import TIME_SERIES_METADATA_TABLE
from infrasys.db_migrations import metadata_needs_migration, migrate_legacy_schema
from infrasys.time_series_metadata_store import TimeSeriesMetadataStore
from infrasys.utils.sqlite import create_in_memory_db, execute

from .models.simple_system import SimpleSystem


@pytest.fixture
def legacy_system(pytestconfig):
    return pytestconfig.rootpath.joinpath("tests/data/legacy_system.json")


def test_metadata_version_detection():
    conn = create_in_memory_db()
    metadata_store = TimeSeriesMetadataStore(conn, initialize=True)

    assert isinstance(metadata_store, TimeSeriesMetadataStore)
    assert not metadata_needs_migration(conn)


def test_migrate_old_system(legacy_system):
    system = SimpleSystem.from_json(legacy_system)
    conn = system._time_series_mgr._metadata_store._con
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    assert "time_series_associations" in tables


def test_migrate_without_columns(legacy_system):
    conn = create_in_memory_db()
    conn.execute(f"CREATE TABlE {TIME_SERIES_METADATA_TABLE}(id, test)")
    with pytest.raises(NotImplementedError):
        migrate_legacy_schema(conn)


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
        "user_attributes_hash",
        "metadata",
    ]
    conn = create_in_memory_db()
    schema_text = ",".join(legacy_columns)
    cur = conn.cursor()
    execute(cur, f"CREATE TABLE {TIME_SERIES_METADATA_TABLE}({schema_text})")
    conn.commit()
    assert migrate_legacy_schema(conn)
