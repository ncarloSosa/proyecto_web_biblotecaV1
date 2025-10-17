"""DAO for PRESTAMO."""
from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, query_all, query_one


def _next_id() -> int:
    row = query_one("SELECT NVL(MAX(ID_PRESTAMO), 0) + 1 AS ID FROM PRESTAMO")
    return int(row["ID"]) if row else 1


def listar() -> List[Dict[str, object]]:
    return query_all("SELECT * FROM PRESTAMO ORDER BY ID_PRESTAMO DESC")


def obtener(id_prestamo: int) -> Optional[Dict[str, object]]:
    return query_one("SELECT * FROM PRESTAMO WHERE ID_PRESTAMO = :id", {"id": id_prestamo})


def crear(data: Dict[str, object]) -> int:
    data = {**data}
    data.setdefault("ID_PRESTAMO", _next_id())
    sql = (
        "INSERT INTO PRESTAMO (ID_PRESTAMO, FECHA_PRESTADO, FECH_CADUC, ESTADO, ESTADO_FISIC, ID_LIBRO, ID_USUARIO) "
        "VALUES (:ID_PRESTAMO, :FECHA_PRESTADO, :FECH_CADUC, :ESTADO, :ESTADO_FISIC, :ID_LIBRO, :ID_USUARIO)"
    )
    execute(sql, data)
    return int(data["ID_PRESTAMO"])


def actualizar(id_prestamo: int, data: Dict[str, object]) -> None:
    sql = (
        "UPDATE PRESTAMO SET FECHA_PRESTADO = :FECHA_PRESTADO, FECH_CADUC = :FECH_CADUC, "
        "ESTADO = :ESTADO, ESTADO_FISIC = :ESTADO_FISIC, ID_LIBRO = :ID_LIBRO, ID_USUARIO = :ID_USUARIO "
        "WHERE ID_PRESTAMO = :ID_PRESTAMO"
    )
    payload = {**data, "ID_PRESTAMO": id_prestamo}
    execute(sql, payload)


def eliminar(id_prestamo: int) -> None:
    execute("DELETE FROM PRESTAMO WHERE ID_PRESTAMO = :id", {"id": id_prestamo})
