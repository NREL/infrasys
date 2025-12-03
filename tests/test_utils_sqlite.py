from __future__ import annotations

import gc
from pathlib import Path

from infrasys.utils.sqlite import (
    ManagedConnection,
    backup,
    create_in_memory_db,
    execute,
    restore,
)


def test_create_in_memory_db_is_managed_connection() -> None:
    with create_in_memory_db() as con:
        assert isinstance(con, ManagedConnection)
        cur = con.cursor()
        execute(cur, "CREATE TABLE test (id INTEGER)")
        execute(cur, "INSERT INTO test VALUES (?)", (1,))
        con.commit()

    # Close is idempotent
    con.close()


def test_backup_and_restore(tmp_path: Path) -> None:
    src = create_in_memory_db()
    cur = src.cursor()
    execute(cur, "CREATE TABLE t (val INTEGER)")
    execute(cur, "INSERT INTO t VALUES (42)")
    src.commit()

    backup_file = tmp_path / "backup.db"
    backup(src, backup_file)

    dst = create_in_memory_db()
    restore(dst, backup_file)
    val = dst.execute("SELECT val FROM t").fetchone()[0]
    assert val == 42

    src.close()
    dst.close()


def test_connection_auto_close_on_gc() -> None:
    con: ManagedConnection | None = create_in_memory_db()
    assert con is not None
    assert con.__dict__.get("_closed", False) is False
    # Explicitly invoke cleanup to exercise __del__ path.
    con.__del__()  # type: ignore[operator]
    assert con.__dict__.get("_closed", False) is True

    con = None  # noqa: PLW0642
    gc.collect()
