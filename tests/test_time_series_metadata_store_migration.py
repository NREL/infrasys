import pytest

from infrasys.db_migrations import needs_migration
from infrasys.time_series_metadata_store import TimeSeriesMetadataStore
from infrasys.utils.sqlite import create_in_memory_db

from .models.simple_system import SimpleSystem


@pytest.fixture
def legacy_system(pytestconfig):
    return pytestconfig.rootpath.joinpath("tests/data/legacy_system.json")


def test_metadata_version_detection():
    conn = create_in_memory_db()
    metadata_store = TimeSeriesMetadataStore(conn, initialize=True)

    assert isinstance(metadata_store, TimeSeriesMetadataStore)
    assert not needs_migration(conn)


def test_migrate_old_system(legacy_system):
    system = SimpleSystem.from_json(legacy_system)
    conn = system._time_series_mgr._metadata_store._con
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    assert "time_series_associations" in tables
