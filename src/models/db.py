"""Database connection utilities for Oracle."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Dict, Iterable, List, Optional

import oracledb

from config import Config


_pool: Optional[oracledb.ConnectionPool] = None


def _get_pool() -> oracledb.ConnectionPool:
    global _pool
    if _pool is None:
        config = Config()
        if not all([config.ORACLE_USER, config.ORACLE_PASSWORD, config.ORACLE_DSN]):
            raise RuntimeError("Oracle connection details are not fully configured")
        _pool = oracledb.create_pool(
            user=config.ORACLE_USER,
            password=config.ORACLE_PASSWORD,
            dsn=config.ORACLE_DSN,
            min=config.ORACLE_POOL_MIN,
            max=config.ORACLE_POOL_MAX,
            increment=1,
        )
    return _pool


@contextmanager
def get_conn():
    """Context manager yielding a pooled connection."""

    pool = _get_pool()
    conn = pool.acquire()
    try:
        yield conn
    finally:
        conn.close()


def execute(sql: str, binds: Optional[Dict[str, object]] = None) -> None:
    """Execute a DDL/DML statement and commit the transaction."""

    binds = binds or {}
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, binds)
        conn.commit()


def _rows_to_dicts(cursor, rows: Iterable[Iterable[object]]) -> List[Dict[str, object]]:
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def query_all(sql: str, binds: Optional[Dict[str, object]] = None) -> List[Dict[str, object]]:
    binds = binds or {}
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, binds)
            rows = cursor.fetchall()
            return _rows_to_dicts(cursor, rows)


def query_one(sql: str, binds: Optional[Dict[str, object]] = None) -> Optional[Dict[str, object]]:
    binds = binds or {}
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, binds)
            row = cursor.fetchone()
            if row is None:
                return None
            return _rows_to_dicts(cursor, [row])[0]


def table_exists(table: str) -> bool:
    sql = """
      SELECT COUNT(*) C
        FROM USER_TABLES
       WHERE TABLE_NAME = :t
    """
    row = query_one(sql, {"t": table.upper()})
    return (row or {}).get("C", 0) > 0


def first_existing_table(candidates: List[str]) -> str:
    for table in candidates:
        if table_exists(table):
            return table
    raise RuntimeError(f"Ninguna tabla de {candidates} existe")


def next_id(table: str, pk: str) -> int:
    """Compute the next numeric identifier for ``table.pk``."""

    sql = f"SELECT NVL(MAX({pk}), 0) + 1 AS ID FROM {table}"
    row = query_one(sql)
    return int(row["ID"]) if row else 1


def column_exists(table: str, column: str) -> bool:
    """Return ``True`` if ``table.column`` exists in the current schema."""

    sql = """
      SELECT COUNT(*) AS C
        FROM USER_TAB_COLUMNS
       WHERE TABLE_NAME = :T
         AND COLUMN_NAME = :C
    """
    row = query_one(sql, {"T": table.upper(), "C": column.upper()})
    return (row or {}).get("C", 0) > 0


def first_existing_column(table: str, candidates: List[str]) -> str:
    """Return the first column from ``candidates`` that exists in ``table``.

    Raises ``RuntimeError`` if none of the provided column names are present.
    """

    for candidate in candidates:
        if column_exists(table, candidate):
            return candidate
    raise RuntimeError(f"Ninguna columna de {candidates} existe en {table}")


def get_col_datatype(table: str, column: str) -> Optional[str]:
    sql = """
      SELECT DATA_TYPE
        FROM USER_TAB_COLUMNS
       WHERE TABLE_NAME = :t
         AND COLUMN_NAME = :c
    """
    row = query_one(sql, {"t": table.upper(), "c": column.upper()})
    return (row or {}).get("DATA_TYPE")


def find_fk_to(table: str, ref_table: str) -> Optional[str]:
    sql = """
      SELECT a.COLUMN_NAME
        FROM USER_CONS_COLUMNS a
        JOIN USER_CONSTRAINTS c ON a.CONSTRAINT_NAME = c.CONSTRAINT_NAME
        JOIN USER_CONSTRAINTS r ON c.R_CONSTRAINT_NAME = r.CONSTRAINT_NAME
        JOIN USER_CONS_COLUMNS rc ON r.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
       WHERE c.CONSTRAINT_TYPE = 'R'
         AND a.TABLE_NAME = :t
         AND r.TABLE_NAME = :rt
       FETCH FIRST 1 ROWS ONLY
    """
    row = query_one(sql, {"t": table.upper(), "rt": ref_table.upper()})
    return (row or {}).get("COLUMN_NAME")


def any_column_like(table: str, patterns: List[str]) -> Optional[str]:
    for pattern in patterns:
        row = query_one(
            """
          SELECT COLUMN_NAME
            FROM USER_TAB_COLUMNS
           WHERE TABLE_NAME = :t
             AND COLUMN_NAME LIKE :p
           FETCH FIRST 1 ROWS ONLY
        """,
            {"t": table.upper(), "p": f"%{pattern.upper()}%"},
        )
        if row and row.get("COLUMN_NAME"):
            return row["COLUMN_NAME"]
    return None
