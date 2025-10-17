"""DAO helpers for PRESTAMO."""

from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, query_all, query_one


def listar() -> List[Dict[str, object]]:
    sql = """
        SELECT ID_PRESTAMO,
               FECHA_PRESTADO,
               FECHA_CADUCIDAD,
               ESTADO,
               ESTADO_FISICO,
               LIBRO_ID_LIBRO,
               USUARIO_ID_USUARIO
          FROM PRESTAMO
         ORDER BY ID_PRESTAMO DESC
    """
    return query_all(sql)


def obtener(id_prestamo: int) -> Optional[Dict[str, object]]:
    sql = """
        SELECT ID_PRESTAMO,
               FECHA_PRESTADO,
               FECHA_CADUCIDAD,
               ESTADO,
               ESTADO_FISICO,
               LIBRO_ID_LIBRO,
               USUARIO_ID_USUARIO
          FROM PRESTAMO
         WHERE ID_PRESTAMO = :ID
    """
    return query_one(sql, {"ID": id_prestamo})


def crear(data: Dict[str, object]) -> None:
    sql = """
        INSERT INTO PRESTAMO (
            ID_PRESTAMO,
            FECHA_PRESTADO,
            FECHA_CADUCIDAD,
            ESTADO,
            ESTADO_FISICO,
            LIBRO_ID_LIBRO,
            USUARIO_ID_USUARIO
        ) VALUES (
            (SELECT NVL(MAX(ID_PRESTAMO),0)+1 FROM PRESTAMO),
            TO_DATE(:FECHA_PRESTADO,'YYYY-MM-DD'),
            TO_DATE(:FECHA_CADUCIDAD,'YYYY-MM-DD'),
            :ESTADO,
            :ESTADO_FISICO,
            :LIBRO_ID_LIBRO,
            :USUARIO_ID_USUARIO
        )
    """
    execute(sql, data)


def actualizar(id_prestamo: int, data: Dict[str, object]) -> None:
    payload = {**data, "ID": id_prestamo}
    sql = """
        UPDATE PRESTAMO
           SET FECHA_PRESTADO = TO_DATE(:FECHA_PRESTADO,'YYYY-MM-DD'),
               FECHA_CADUCIDAD = TO_DATE(:FECHA_CADUCIDAD,'YYYY-MM-DD'),
               ESTADO = :ESTADO,
               ESTADO_FISICO = :ESTADO_FISICO,
               LIBRO_ID_LIBRO = :LIBRO_ID_LIBRO,
               USUARIO_ID_USUARIO = :USUARIO_ID_USUARIO
         WHERE ID_PRESTAMO = :ID
    """
    execute(sql, payload)


def eliminar(id_prestamo: int) -> None:
    execute("DELETE FROM PRESTAMO WHERE ID_PRESTAMO = :ID", {"ID": id_prestamo})
