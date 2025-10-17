"""DAO for EDITORIAL."""
from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, query_all, query_one


def _next_id() -> int:
    row = query_one("SELECT NVL(MAX(ID_EDITORIAL), 0) + 1 AS ID FROM EDITORIAL")
    return int(row["ID"]) if row else 1


def listar() -> List[Dict[str, object]]:
    sql = """
        SELECT ID_EDITORIAL, NOMBRE, PAIS, ANO_EDICION, NUM_EDITORIAL
        FROM EDITORIAL
        ORDER BY ID_EDITORIAL DESC
    """
    return query_all(sql)


def obtener(id_editorial: int) -> Optional[Dict[str, object]]:
    sql = """
        SELECT ID_EDITORIAL, NOMBRE, PAIS, ANO_EDICION, NUM_EDITORIAL
        FROM EDITORIAL
        WHERE ID_EDITORIAL = :id
    """
    return query_one(sql, {"id": id_editorial})


def crear(data: Dict[str, object]) -> int:
    data = {**data}
    data.setdefault("ID_EDITORIAL", _next_id())
    sql = """
        INSERT INTO EDITORIAL (ID_EDITORIAL, NOMBRE, PAIS)
        VALUES (:ID_EDITORIAL, :NOMBRE, :PAIS)
    """
    execute(sql, data)
    return int(data["ID_EDITORIAL"])


def actualizar(id_editorial: int, data: Dict[str, object]) -> None:
    sql = (
        "UPDATE EDITORIAL SET NOMBRE = :NOMBRE, PAIS = :PAIS "
        "WHERE ID_EDITORIAL = :ID_EDITORIAL"
    )
    payload = {**data, "ID_EDITORIAL": id_editorial}
    execute(sql, payload)


def eliminar(id_editorial: int) -> None:
    execute("DELETE FROM EDITORIAL WHERE ID_EDITORIAL = :id", {"id": id_editorial})
