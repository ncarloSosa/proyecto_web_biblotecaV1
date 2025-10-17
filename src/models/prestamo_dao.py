"""DAO helpers for PRESTAMO."""

from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, query_all, query_one


def _next_id() -> int:
    row = query_one("SELECT NVL(MAX(ID_PRESTAMO), 0) + 1 AS ID FROM PRESTAMO")
    return int(row["ID"]) if row else 1


def listar() -> List[Dict[str, object]]:
    sql = """
        SELECT ID_PRESTAMO, FECHA_PRESTADO, FECHA_CADUCIDAD, ESTADO, ESTADO_FISICO,
               USUARIO_ID_USUARIO, LIBRO_ID_LIBRO
        FROM PRESTAMO
        ORDER BY ID_PRESTAMO DESC
    """
    return query_all(sql)


def obtener(id_prestamo: int) -> Optional[Dict[str, object]]:
    sql = """
        SELECT ID_PRESTAMO, FECHA_PRESTADO, FECHA_CADUCIDAD, ESTADO, ESTADO_FISICO,
               USUARIO_ID_USUARIO, LIBRO_ID_LIBRO
        FROM PRESTAMO
        WHERE ID_PRESTAMO = :id
    """
    return query_one(sql, {"id": id_prestamo})


def crear(data: Dict[str, object]) -> int:
    payload = {**data}
    payload.setdefault("id", _next_id())
    sql = """
    INSERT INTO PRESTAMO (
      ID_PRESTAMO, FECHA_PRESTADO, FECHA_CADUCIDAD, ESTADO, ESTADO_FISICO,
      USUARIO_ID_USUARIO, LIBRO_ID_LIBRO
    ) VALUES (
      :id, :fecha_prestado, :fecha_caducidad, :estado, :estado_fisico,
      :usuario_id, :libro_id
    )
    """
    execute(sql, payload)
    return int(payload["id"])


def actualizar(id_prestamo: int, data: Dict[str, object]) -> None:
    payload = {**data, "id": id_prestamo}
    sql = """
    UPDATE PRESTAMO
    SET FECHA_PRESTADO = :fecha_prestado,
        FECHA_CADUCIDAD = :fecha_caducidad,
        ESTADO = :estado,
        ESTADO_FISICO = :estado_fisico,
        USUARIO_ID_USUARIO = :usuario_id,
        LIBRO_ID_LIBRO = :libro_id
    WHERE ID_PRESTAMO = :id
    """
    execute(sql, payload)


def eliminar(id_prestamo: int) -> None:
    execute("DELETE FROM PRESTAMO WHERE ID_PRESTAMO = :id", {"id": id_prestamo})
