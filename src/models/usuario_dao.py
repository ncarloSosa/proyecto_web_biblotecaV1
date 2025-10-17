"""DAO helpers for the USUARIO table."""

from __future__ import annotations

from functools import lru_cache
from typing import Dict, Iterable, List, Optional, Sequence, Set

from .db import execute, query_all, query_one


def _next_id() -> int:
    row = query_one("SELECT NVL(MAX(ID_USUARIO), 0) + 1 AS ID FROM USUARIO")
    return int(row["ID"]) if row else 1


@lru_cache(maxsize=None)
def _cols_present(table: str) -> Set[str]:
    rows = query_all(
        """
        SELECT COLUMN_NAME
        FROM USER_TAB_COLUMNS
        WHERE TABLE_NAME = :t
    """,
        {"t": table.upper()},
    )
    return {row["COLUMN_NAME"] for row in rows}


def columnas_disponibles() -> Set[str]:
    """Return the set of columns available in USUARIO."""

    return _cols_present("USUARIO")


def _select_columns(base: Sequence[str]) -> Iterable[str]:
    cols = columnas_disponibles()
    for col in base:
        if col in cols:
            yield col


def listar() -> List[Dict[str, object]]:
    select_cols = list(_select_columns(["ID_USUARIO", "NOMBRE", "ROL", "FECHA_REGISTRO"]))
    if "ID_USUARIO" not in select_cols:
        raise RuntimeError("USUARIO table must include ID_USUARIO column")
    sql = f"SELECT {', '.join(select_cols)} FROM USUARIO ORDER BY ID_USUARIO DESC"
    return query_all(sql)


def obtener(id_usuario: int) -> Optional[Dict[str, object]]:
    select_cols = list(_select_columns(["ID_USUARIO", "NOMBRE", "ROL", "FECHA_REGISTRO"]))
    if not select_cols:
        return None
    sql = f"SELECT {', '.join(select_cols)} FROM USUARIO WHERE ID_USUARIO = :id"
    return query_one(sql, {"id": id_usuario})


def buscar_por_nombre(nombre: str) -> Optional[Dict[str, object]]:
    cols = columnas_disponibles()
    select_parts = ["ID_USUARIO", "NOMBRE"]
    if "ROL" in cols:
        select_parts.append("ROL")
    pwd_col = next((cand for cand in ("CONTRASENA", "CLAVE", "PASSWORD") if cand in cols), None)
    if pwd_col:
        select_parts.append(f"{pwd_col} AS PWD")
    column_list = ", ".join(select_parts)
    sql = f"""
        SELECT {column_list}
        FROM USUARIO
        WHERE UPPER(NOMBRE) = UPPER(:nombre)
        FETCH FIRST 1 ROWS ONLY
    """
    return query_one(sql, {"nombre": nombre})


def crear(data: Dict[str, object]) -> int:
    data = {key.upper(): value for key, value in data.items()}
    data.setdefault("ID_USUARIO", _next_id())
    available = columnas_disponibles()
    columns = ["ID_USUARIO"]
    for field in ("NOMBRE", "ROL", "FECHA_REGISTRO"):
        if field in available and field in data and data[field] is not None:
            columns.append(field)
    placeholders = ", ".join(f":{col}" for col in columns)
    sql = f"INSERT INTO USUARIO ({', '.join(columns)}) VALUES ({placeholders})"
    execute(sql, data)
    return int(data["ID_USUARIO"])


def actualizar(id_usuario: int, data: Dict[str, object]) -> None:
    data = {key.upper(): value for key, value in data.items()}
    available = columnas_disponibles()
    assignments = []
    payload = {"ID_USUARIO": id_usuario}
    for field in ("NOMBRE", "ROL", "FECHA_REGISTRO"):
        if field in available and field in data and data[field] is not None:
            assignments.append(f"{field} = :{field}")
            payload[field] = data[field]
    if not assignments:
        return
    sql = f"UPDATE USUARIO SET {', '.join(assignments)} WHERE ID_USUARIO = :ID_USUARIO"
    execute(sql, payload)


def eliminar(id_usuario: int) -> None:
    execute("DELETE FROM USUARIO WHERE ID_USUARIO = :id", {"id": id_usuario})
