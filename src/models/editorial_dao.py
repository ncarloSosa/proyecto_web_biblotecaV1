"""DAO for EDITORIAL."""
from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, query_all, query_one


def _next_id() -> int:
    row = query_one("SELECT NVL(MAX(ID_VAREDIT), 0) + 1 AS ID FROM EDITORIAL")
    return int(row["ID"]) if row else 1


def listar() -> List[Dict[str, object]]:
    return query_all("SELECT * FROM EDITORIAL ORDER BY ID_VAREDIT DESC")


def obtener(id_varedit: int) -> Optional[Dict[str, object]]:
    return query_one("SELECT * FROM EDITORIAL WHERE ID_VAREDIT = :id", {"id": id_varedit})


def crear(data: Dict[str, object]) -> int:
    data = {**data}
    data.setdefault("ID_VAREDIT", _next_id())
    sql = "INSERT INTO EDITORIAL (ID_VAREDIT, NOMBRE, PAIS) VALUES (:ID_VAREDIT, :NOMBRE, :PAIS)"
    execute(sql, data)
    return int(data["ID_VAREDIT"])


def actualizar(id_varedit: int, data: Dict[str, object]) -> None:
    sql = (
        "UPDATE EDITORIAL SET NOMBRE = :NOMBRE, PAIS = :PAIS "
        "WHERE ID_VAREDIT = :ID_VAREDIT"
    )
    payload = {**data, "ID_VAREDIT": id_varedit}
    execute(sql, payload)


def eliminar(id_varedit: int) -> None:
    execute("DELETE FROM EDITORIAL WHERE ID_VAREDIT = :id", {"id": id_varedit})
