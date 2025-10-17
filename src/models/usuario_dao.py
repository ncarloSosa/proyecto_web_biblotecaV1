"""DAO for USUARIO."""
from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, query_all, query_one


def _next_id() -> int:
    row = query_one("SELECT NVL(MAX(ID_USUARIO), 0) + 1 AS ID FROM USUARIO")
    return int(row["ID"]) if row else 1


def listar() -> List[Dict[str, object]]:
    sql = """
        SELECT ID_USUARIO, NOMBRE, ROL, FECHA_REGISTRO
        FROM USUARIO
        ORDER BY ID_USUARIO DESC
    """
    return query_all(sql)


def obtener(id_usuario: int) -> Optional[Dict[str, object]]:
    sql = """
        SELECT ID_USUARIO, NOMBRE, ROL, FECHA_REGISTRO
        FROM USUARIO
        WHERE ID_USUARIO = :id
    """
    return query_one(sql, {"id": id_usuario})


def buscar_por_nombre(nombre: str) -> Optional[Dict[str, object]]:
    sql = """
        SELECT ID_USUARIO, NOMBRE, ROL, FECHA_REGISTRO
        FROM USUARIO
        WHERE UPPER(NOMBRE) = UPPER(:nombre)
    """
    return query_one(sql, {"nombre": nombre})


def crear(data: Dict[str, object]) -> int:
    data = {**data}
    data.setdefault("ID_USUARIO", _next_id())
    sql = (
        "INSERT INTO USUARIO (ID_USUARIO, NOMBRE, ROL, FECHA_REGISTRO) "
        "VALUES (:ID_USUARIO, :NOMBRE, :ROL, :FECHA_REGISTRO)"
    )
    execute(sql, data)
    return int(data["ID_USUARIO"])


def actualizar(id_usuario: int, data: Dict[str, object]) -> None:
    sql = (
        "UPDATE USUARIO SET NOMBRE = :NOMBRE, ROL = :ROL, FECHA_REGISTRO = :FECHA_REGISTRO "
        "WHERE ID_USUARIO = :ID_USUARIO"
    )
    payload = {**data, "ID_USUARIO": id_usuario}
    execute(sql, payload)


def eliminar(id_usuario: int) -> None:
    execute("DELETE FROM USUARIO WHERE ID_USUARIO = :id", {"id": id_usuario})
