"""Database connection pool and query execution.

SECURITY: This module enforces STRICT read-only access.
Under NO circumstances can any data be modified or deleted.
- Connections use SET default_transaction_read_only = ON
- SQL is validated against a comprehensive blocklist before execution
- Only SELECT and WITH (CTE) statements are permitted
- Queries touching the main forecast tables MUST include WHERE and LIMIT clauses
- SELECT * is never allowed
- Queries can be cancelled via their backend PID
"""

from __future__ import annotations

import logging
import re
from typing import Optional, Dict

import psycopg2
import psycopg2.pool
import psycopg2.extras
from contextlib import contextmanager

import config

logger = logging.getLogger(__name__)

_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None

# Every keyword that could modify data — checked before execution.
# NOTE: "LOAD" and "IMPORT" are intentionally excluded from this list.
# They are NOT data-modification commands and are common column/alias names
# in the energy domain (e.g., electrical load in MW). They are also already
# covered by the SELECT/WITH start requirement — the SQL LOAD command can
# only appear as a top-level statement, which that check blocks.
_FORBIDDEN_KEYWORDS = frozenset({
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE",
    "GRANT", "REVOKE", "REPLACE", "UPSERT", "MERGE",
    "COPY",
    "EXECUTE", "EXEC", "CALL",
    "SET ROLE", "SET SESSION AUTHORIZATION", "RESET ROLE",
    "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT",
    "LOCK", "VACUUM", "ANALYZE", "REINDEX", "CLUSTER",
    "COMMENT", "SECURITY", "REASSIGN", "DISCARD",
    "DO", "NOTIFY", "LISTEN", "UNLISTEN",
    "PREPARE", "DEALLOCATE",
})

# The four main forecast tables — all contain billions of rows.
# Any SQL touching these tables is subject to stricter structural checks.
_MAIN_TABLES = frozenset({
    "WEATHER_FORECAST_ENSEMBLE",
    "WEATHER_SEASONAL_ENSEMBLE",
    "ENERGY_BASE_ENSEMBLE",
    "ENERGY_FORECAST_ENSEMBLE",
})

# Track active queries for cancellation: request_id -> backend_pid
_active_queries: Dict[str, int] = {}


def init_pool(min_conn: int = 2, max_conn: int = 10):
    """Initialize the connection pool. All connections are forced read-only."""
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
    logger.info("Database connection pool initialized (%d-%d connections, READ-ONLY enforced)", min_conn, max_conn)


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


def _references_main_table(cleaned_upper: str) -> bool:
    """Return True if the SQL touches any of the main billion-row forecast tables."""
    return any(table in cleaned_upper for table in _MAIN_TABLES)


def _validate_sql(sql: str):
    """
    Validate SQL for safety and scope before execution.

    Two categories of checks:

    1. SECURITY checks (always applied):
       - Must be SELECT or WITH — no writes possible
       - Forbidden keyword blocklist
       - No multi-statement injection
       - No dangerous function/pattern injection

    2. SCOPE checks (applied when a main forecast table is referenced):
       - No SELECT * — must name specific columns
       - Must have a WHERE clause — no full table scans
       - Must have a LIMIT clause — no unbounded result dumps
    """
    # Strip comments to prevent bypass via comment injection
    cleaned = re.sub(r'--.*?$', ' ', sql, flags=re.MULTILINE)   # single-line comments
    cleaned = re.sub(r'/\*.*?\*/', ' ', cleaned, flags=re.DOTALL)  # block comments
    cleaned_upper = cleaned.strip().upper()

    # ── Security check 1: Must start with SELECT or WITH ──────────────
    if not cleaned_upper.startswith("SELECT") and not cleaned_upper.startswith("WITH"):
        raise ValueError("BLOCKED: Only SELECT / WITH queries are allowed. No data modification permitted.")

    # ── Security check 2: Forbidden keyword blocklist ─────────────────
    tokens = re.findall(r'[A-Z_]+', cleaned_upper)
    for token in tokens:
        if token in _FORBIDDEN_KEYWORDS:
            raise ValueError(f"BLOCKED: Forbidden keyword '{token}' detected. No data modification permitted.")

    # ── Security check 3: No multi-statement injection ────────────────
    statements = [s.strip() for s in cleaned.split(';') if s.strip()]
    if len(statements) > 1:
        raise ValueError("BLOCKED: Multiple SQL statements are not allowed.")

    # ── Security check 4: Injection pattern blocklist ─────────────────
    injection_patterns = [
        r"INTO\s+(?:OUTFILE|DUMPFILE)",
        r"LOAD_FILE\s*\(",
        r"pg_sleep\s*\(",
        r"dblink\s*\(",
        r"pg_read_file\s*\(",
        r"pg_write_file\s*\(",
        r"lo_import\s*\(",
        r"lo_export\s*\(",
    ]
    for pattern in injection_patterns:
        if re.search(pattern, cleaned_upper):
            raise ValueError("BLOCKED: Potentially dangerous SQL pattern detected.")

    # ── Scope checks: only for queries that touch the main tables ──────
    # These checks prevent full scans and unrestricted data dumps on the
    # billion-row forecast tables.  Simple utility queries (SELECT 1, etc.)
    # are not subject to them.
    if _references_main_table(cleaned_upper):

        # No SELECT * — forces the LLM to select only what it needs
        if re.search(r'SELECT\s+\*', cleaned_upper):
            raise ValueError(
                "BLOCKED: SELECT * is not permitted on forecast tables. "
                "Please select specific columns."
            )

        # Require at least one WHERE clause anywhere in the SQL.
        # Covers both direct queries and CTEs (WHERE lives inside the CTE body).
        if not re.search(r'\bWHERE\b', cleaned_upper):
            raise ValueError(
                "BLOCKED: Queries on forecast tables must include a WHERE clause "
                "to filter by project_name, location, variable, and/or time range. "
                "Full table scans are not permitted."
            )

        # Require a LIMIT clause anywhere in the SQL.
        # Even if the Python layer truncates results, the DB query itself must be
        # bounded to prevent exhausting server resources on full scans.
        if not re.search(r'\bLIMIT\s+\d+', cleaned_upper):
            raise ValueError(
                "BLOCKED: Queries on forecast tables must include a LIMIT clause "
                "to cap the number of rows returned. "
                f"Maximum allowed: {config.MAX_QUERY_ROWS} rows."
            )


def execute_query(sql: str, params: Optional[dict] = None, request_id: Optional[str] = None) -> dict:
    """
    Execute a STRICTLY read-only SQL query and return results.

    Security layers:
      1. SQL text validation (keyword blocklist, comment stripping, injection detection)
      2. SET default_transaction_read_only = ON (database-level enforcement)
      3. Statement timeout to prevent resource exhaustion

    Returns:
        {"columns": [...], "rows": [[...], ...], "row_count": int, "truncated": bool}
    """
    _validate_sql(sql)

    with get_connection() as conn:
        with conn.cursor() as cur:
            # CRITICAL: Force the transaction to read-only at the database level.
            # Even if SQL validation is somehow bypassed, PostgreSQL will reject writes.
            cur.execute("SET default_transaction_read_only = ON")
            cur.execute(f"SET LOCAL statement_timeout = '{config.QUERY_TIMEOUT_SEC * 1000}'")

            # Track this query's backend PID for cancellation
            cur.execute("SELECT pg_backend_pid()")
            backend_pid = cur.fetchone()[0]
            if request_id:
                _active_queries[request_id] = backend_pid

            try:
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
            finally:
                if request_id:
                    _active_queries.pop(request_id, None)


def cancel_query(request_id: str) -> bool:
    """
    Cancel a running query by its request_id.
    Returns True if cancellation was sent, False if no active query found.
    """
    backend_pid = _active_queries.pop(request_id, None)
    if backend_pid is None:
        return False

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_cancel_backend(%s)", (backend_pid,))
                result = cur.fetchone()[0]
                logger.info("Cancelled query for request %s (pid=%d, result=%s)", request_id, backend_pid, result)
                return result
    except Exception as e:
        logger.error("Failed to cancel query for request %s: %s", request_id, e)
        return False
