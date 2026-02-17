"""Database connection pool and query execution."""

from __future__ import annotations

import logging
from typing import Optional

import psycopg2
import psycopg2.pool
import psycopg2.extras
from contextlib import contextmanager

import config

logger = logging.getLogger(__name__)

_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None


def init_pool(min_conn: int = 2, max_conn: int = 10):
    """Initialize the connection pool."""
    global _pool
    _pool = psycopg2.pool.ThreadedConnectionPool(
        min_conn,
        max_conn,
        host=config.DB_HOST,
        port=config.DB_PORT,
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        sslmode=config.DB_SSLMODE,
        connect_timeout=10,
    )
    logger.info("Database connection pool initialized (%d-%d connections)", min_conn, max_conn)


def close_pool():
    """Close all connections in the pool."""
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None


@contextmanager
def get_connection():
    """Get a connection from the pool (context manager)."""
    conn = _pool.getconn()
    try:
        yield conn
    finally:
        _pool.putconn(conn)


def execute_query(sql: str, params: Optional[dict] = None) -> dict:
    """
    Execute a read-only SQL query and return results as a dict.

    Returns:
        {
            "columns": ["col1", "col2", ...],
            "rows": [[val1, val2, ...], ...],
            "row_count": int,
            "truncated": bool
        }
    """
    sql_upper = sql.strip().upper()
    if not sql_upper.startswith("SELECT") and not sql_upper.startswith("WITH"):
        raise ValueError("Only SELECT / WITH queries are allowed")

    for forbidden in ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE", "GRANT", "REVOKE"):
        if forbidden in sql_upper.split("--")[0].split("/*")[0]:
            tokens = sql_upper.replace("(", " ").replace(")", " ").split()
            if forbidden in tokens:
                raise ValueError(f"Forbidden SQL keyword: {forbidden}")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL statement_timeout = '{config.QUERY_TIMEOUT_SEC * 1000}'")
            cur.execute(sql, params or {})
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchmany(config.MAX_QUERY_ROWS + 1)

            truncated = len(rows) > config.MAX_QUERY_ROWS
            if truncated:
                rows = rows[: config.MAX_QUERY_ROWS]

            serialized_rows = []
            for row in rows:
                serialized_row = []
                for val in row:
                    if hasattr(val, "isoformat"):
                        serialized_row.append(val.isoformat())
                    elif val is None:
                        serialized_row.append(None)
                    else:
                        serialized_row.append(val)
                serialized_rows.append(serialized_row)

            return {
                "columns": columns,
                "rows": serialized_rows,
                "row_count": len(serialized_rows),
                "truncated": truncated,
            }
