"""Data access helpers for AUTOR."""
from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, query_all, query_one


def _next_id() -> int:
    row = query_one("SELECT NVL(MAX(ID_AUTOR), 0) + 1 AS ID FROM AUTOR")
    return int(row["ID"]) if row else 1


def listar() -> List[Dict[str, object]]:
    return query_all("SELECT * FROM AUTOR ORDER BY ID_AUTOR DESC")


def obtener(id_autor: int) -> Optional[Dict[str, object]]:
    return query_one("SELECT * FROM AUTOR WHERE ID_AUTOR = :id", {"id": id_autor})


def crear(data: Dict[str, object]) -> int:
    data = {**data}
    data.setdefault("ID_AUTOR", _next_id())
    sql = (
        "INSERT INTO AUTOR (ID_AUTOR, NOMBRE, APELLIDO, FECH_NACIMIENT, NACIONALIDAD, BIOGRAFIA) "
        "VALUES (:ID_AUTOR, :NOMBRE, :APELLIDO, :FECH_NACIMIENT, :NACIONALIDAD, :BIOGRAFIA)"
    )
    execute(sql, data)
    return int(data["ID_AUTOR"])


def actualizar(id_autor: int, data: Dict[str, object]) -> None:
    sql = (
        "UPDATE AUTOR SET NOMBRE = :NOMBRE, APELLIDO = :APELLIDO, "
        "FECH_NACIMIENT = :FECH_NACIMIENT, NACIONALIDAD = :NACIONALIDAD, BIOGRAFIA = :BIOGRAFIA "
        "WHERE ID_AUTOR = :ID_AUTOR"
    )
    payload = {**data, "ID_AUTOR": id_autor}
    execute(sql, payload)


def eliminar(id_autor: int) -> None:
    execute("DELETE FROM AUTOR WHERE ID_AUTOR = :id", {"id": id_autor})
