"""Data access helpers for the USUARIO table."""

from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, query_all, query_one


_BASE_SELECT = (
    "SELECT ID_USUARIO, USUARIO, NOMBRE, DIRECCION, TELEFONO, DPI, SEXO, "
    "FECHA_CREACION, CONTRASENA FROM USUARIO"
)


def listar() -> List[Dict[str, object]]:
    """Return all users ordered from newest to oldest."""

    sql = f"{_BASE_SELECT} ORDER BY ID_USUARIO DESC"
    return query_all(sql)


def obtener(id_usuario: int) -> Optional[Dict[str, object]]:
    """Fetch a single user by primary key."""

    sql = f"{_BASE_SELECT} WHERE ID_USUARIO = :id"
    return query_one(sql, {"id": id_usuario})


def buscar_por_nombre(nombre: str) -> Optional[Dict[str, object]]:
    """Find a user by login name (USUARIO column)."""

    sql = """
    SELECT ID_USUARIO, USUARIO, CONTRASENA, NOMBRE
    FROM USUARIO
    WHERE UPPER(USUARIO) = UPPER(:nombre)
    FETCH FIRST 1 ROWS ONLY
    """
    return query_one(sql, {"nombre": nombre})


def crear(data: Dict[str, object]) -> None:
    """Insert a new user record."""

    sql = """
    INSERT INTO USUARIO
      (USUARIO, NOMBRE, DIRECCION, TELEFONO, DPI, SEXO, FECHA_CREACION, CONTRASENA)
    VALUES
      (:usuario, :nombre, :direccion, :telefono, :dpi, :sexo, :fecha_creacion, :contrasena)
    """
    execute(sql, data)


def actualizar(id_usuario: int, data: Dict[str, object]) -> None:
    """Update the selected user."""

    payload = {**data, "id": id_usuario}
    sql = """
    UPDATE USUARIO
    SET USUARIO = :usuario,
        NOMBRE = :nombre,
        DIRECCION = :direccion,
        TELEFONO = :telefono,
        DPI = :dpi,
        SEXO = :sexo,
        FECHA_CREACION = :fecha_creacion,
        CONTRASENA = :contrasena
    WHERE ID_USUARIO = :id
    """
    execute(sql, payload)


def eliminar(id_usuario: int) -> None:
    """Delete a user by ID."""

    execute("DELETE FROM USUARIO WHERE ID_USUARIO = :id", {"id": id_usuario})
