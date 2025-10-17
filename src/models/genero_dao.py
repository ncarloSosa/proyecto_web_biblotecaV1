"""DAO for GENERO."""
from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, query_all, query_one


def _next_id() -> int:
    row = query_one("SELECT NVL(MAX(ID_GENERO), 0) + 1 AS ID FROM GENERO")
    return int(row["ID"]) if row else 1


def listar() -> List[Dict[str, object]]:
    return query_all("SELECT * FROM GENERO ORDER BY ID_GENERO DESC")


def obtener(id_genero: int) -> Optional[Dict[str, object]]:
    return query_one("SELECT * FROM GENERO WHERE ID_GENERO = :id", {"id": id_genero})


def crear(data: Dict[str, object]) -> int:
    data = {**data}
    data.setdefault("ID_GENERO", _next_id())
    sql = (
        "INSERT INTO GENERO (ID_GENERO, GENERO, LIBRO_ID_LIBRO) "
        "VALUES (:ID_GENERO, :GENERO, :LIBRO_ID_LIBRO)"
    )
    execute(sql, data)
    return int(data["ID_GENERO"])


def actualizar(id_genero: int, data: Dict[str, object]) -> None:
    sql = (
        "UPDATE GENERO SET GENERO = :GENERO, LIBRO_ID_LIBRO = :LIBRO_ID_LIBRO "
        "WHERE ID_GENERO = :ID_GENERO"
    )
    payload = {**data, "ID_GENERO": id_genero}
    execute(sql, payload)


def eliminar(id_genero: int) -> None:
    execute("DELETE FROM GENERO WHERE ID_GENERO = :id", {"id": id_genero})
