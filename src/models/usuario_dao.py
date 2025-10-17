"""Data access helpers for the USUARIO table."""

from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, query_all, query_one


_BASE_SELECT = """
    SELECT ID_USUARIO,
           USUARIO,
           NOMBRE,
           DIRECCION,
           TELEFONO,
           DPI,
           SEXO,
           FECHA_CREACION,
           CONTRASENA
      FROM USUARIO
"""


def listar() -> List[Dict[str, object]]:
    """Return all users ordered from newest to oldest."""

    sql = f"{_BASE_SELECT} ORDER BY ID_USUARIO DESC"
    return query_all(sql)


def obtener(id_usuario: int) -> Optional[Dict[str, object]]:
    """Fetch a single user by primary key."""

    sql = f"{_BASE_SELECT} WHERE ID_USUARIO = :ID"
    return query_one(sql, {"ID": id_usuario})


def buscar_por_nombre(nombre: str) -> Optional[Dict[str, object]]:
    """Find a user by the NOMBRE column (used for login)."""

    sql = """
        SELECT ID_USUARIO, NOMBRE, CONTRASENA
          FROM USUARIO
         WHERE NOMBRE = :NOMBRE
    """
    return query_one(sql, {"NOMBRE": nombre})


def crear(data: Dict[str, object]) -> None:
    """Insert a new user record."""

    sql = """
        INSERT INTO USUARIO
          (ID_USUARIO, USUARIO, NOMBRE, DIRECCION, TELEFONO, DPI, SEXO, FECHA_CREACION, CONTRASENA)
        VALUES
          (USUARIO_SEQ.NEXTVAL, :USUARIO, :NOMBRE, :DIRECCION, :TELEFONO, :DPI, :SEXO,
           TO_DATE(:FECHA_CREACION,'YYYY-MM-DD'), :CONTRASENA)
    """
    execute(sql, data)


def actualizar(id_usuario: int, data: Dict[str, object]) -> None:
    """Update the selected user."""

    payload = {**data, "ID": id_usuario}
    sql = """
        UPDATE USUARIO
           SET USUARIO = :USUARIO,
               NOMBRE = :NOMBRE,
               DIRECCION = :DIRECCION,
               TELEFONO = :TELEFONO,
               DPI = :DPI,
               SEXO = :SEXO,
               FECHA_CREACION = TO_DATE(:FECHA_CREACION,'YYYY-MM-DD'),
               CONTRASENA = :CONTRASENA
         WHERE ID_USUARIO = :ID
    """
    execute(sql, payload)


def eliminar(id_usuario: int) -> None:
    """Delete a user by ID."""

    execute("DELETE FROM USUARIO WHERE ID_USUARIO = :ID", {"ID": id_usuario})
