"""DAO for GRUPO_LECTURA and related tables."""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from .db import execute, query_all, query_one


def _next_id() -> int:
    row = query_one("SELECT NVL(MAX(ID_GRUPO), 0) + 1 AS ID FROM GRUPO_LECTURA")
    return int(row["ID"]) if row else 1


def listar() -> List[Dict[str, object]]:
    return query_all("SELECT * FROM GRUPO_LECTURA ORDER BY FECHA_REUNION DESC")


def obtener(id_grupo: int) -> Optional[Dict[str, object]]:
    return query_one("SELECT * FROM GRUPO_LECTURA WHERE ID_GRUPO = :id", {"id": id_grupo})


def crear(data: Dict[str, object]) -> int:
    data = {**data}
    data.setdefault("ID_GRUPO", _next_id())
    sql = (
        "INSERT INTO GRUPO_LECTURA (ID_GRUPO, NOMBRE, DESCRIPCION, FECHA_REUNION, HORA_REUNION, LUGAR) "
        "VALUES (:ID_GRUPO, :NOMBRE, :DESCRIPCION, :FECHA_REUNION, :HORA_REUNION, :LUGAR)"
    )
    execute(sql, data)
    return int(data["ID_GRUPO"])


def actualizar(id_grupo: int, data: Dict[str, object]) -> None:
    sql = (
        "UPDATE GRUPO_LECTURA SET NOMBRE = :NOMBRE, DESCRIPCION = :DESCRIPCION, "
        "FECHA_REUNION = :FECHA_REUNION, HORA_REUNION = :HORA_REUNION, LUGAR = :LUGAR "
        "WHERE ID_GRUPO = :ID_GRUPO"
    )
    payload = {**data, "ID_GRUPO": id_grupo}
    execute(sql, payload)


def eliminar(id_grupo: int) -> None:
    execute("DELETE FROM GRUPO_LECTURA WHERE ID_GRUPO = :id", {"id": id_grupo})


def listar_libros(id_grupo: int) -> List[Dict[str, object]]:
    sql = (
        "SELECT GL.ID_LIBRO, L.TITULO "
        "FROM GRUPO_LIBRO GL "
        "JOIN LIBRO L ON L.ID_LIBRO = GL.ID_LIBRO "
        "WHERE GL.ID_GRUPO = :id"
    )
    return query_all(sql, {"id": id_grupo})


def reemplazar_libros(id_grupo: int, lista_ids: Iterable[int]) -> None:
    execute("DELETE FROM GRUPO_LIBRO WHERE ID_GRUPO = :id", {"id": id_grupo})
    for libro_id in lista_ids:
        execute(
            "INSERT INTO GRUPO_LIBRO (ID_GRUPO, ID_LIBRO) VALUES (:ID_GRUPO, :ID_LIBRO)",
            {"ID_GRUPO": id_grupo, "ID_LIBRO": libro_id},
        )
