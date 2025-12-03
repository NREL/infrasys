"""Utility functions for SQLite"""

import sqlite3
from pathlib import Path
from typing import Any, Sequence

from loguru import logger


class ManagedConnection(sqlite3.Connection):
    """SQLite connection that auto-closes on garbage collection."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        super().close()

    def __enter__(self) -> "ManagedConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        super().__exit__(exc_type, exc, tb)

    def __del__(self) -> None:
        self.close()


def backup(src_con: sqlite3.Connection, filename: Path | str) -> None:
    """Backup a database to a file."""
    with sqlite3.connect(filename) as dst_con:
        src_con.backup(dst_con)
    dst_con.close()
    logger.info("Backed up the database to {}.", filename)


def restore(dst_con: sqlite3.Connection, filename: Path | str) -> None:
    """Restore a database from a file."""
    with sqlite3.connect(filename) as src_con:
        src_con.backup(dst_con)
    src_con.close()
    logger.info("Restored the database from {}.", filename)


def create_in_memory_db(database: str = ":memory:") -> sqlite3.Connection:
    """Create an in-memory database."""
    return sqlite3.connect(database, factory=ManagedConnection)


def execute(cursor: sqlite3.Cursor, query: str, params: Sequence[Any] = ()) -> Any:
    """Execute a SQL query."""
    logger.trace("SQL query: {} {}", query, params)
    return cursor.execute(query, params)
