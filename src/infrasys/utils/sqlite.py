"""Utility functions for SQLite"""

import sqlite3
from typing import Any, Sequence

from loguru import logger


def execute(cursor: sqlite3.Cursor, query: str, params: Sequence[str] = ()) -> Any:
    """Execute a SQL query."""
    logger.trace("SQL query: {query} {params=}", query)
    return cursor.execute(query, params)
