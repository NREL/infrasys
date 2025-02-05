"""Utility functions for SQLite"""

import sqlite3
from pathlib import Path
from typing import Any, Sequence

from loguru import logger


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
    return sqlite3.connect(database)


def execute(cursor: sqlite3.Cursor, query: str, params: Sequence[Any] = ()) -> Any:
    """Execute a SQL query."""
    logger.trace("SQL query: {query} {params=}", query)
    return cursor.execute(query, params)
