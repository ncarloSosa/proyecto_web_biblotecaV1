"""DAO helpers for the HISTORIAL table."""

from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, next_id, query_all, query_one


def listar() -> List[Dict[str, object]]:
    sql = """
        SELECT ID_HISTORIAL,
               ACCION,
               FECHA_EVENTO,
               USUARIO_ID_USUARIO,
               LIBRO_ID_LIBRO
          FROM HISTORIAL
         ORDER BY FECHA_EVENTO DESC
    """
    return query_all(sql)


def obtener(id_historial: int) -> Optional[Dict[str, object]]:
    sql = """
        SELECT ID_HISTORIAL,
               ACCION,
               FECHA_EVENTO,
               USUARIO_ID_USUARIO,
               LIBRO_ID_LIBRO
          FROM HISTORIAL
         WHERE ID_HISTORIAL = :ID
    """
    return query_one(sql, {"ID": id_historial})


def crear(data: Dict[str, object]) -> int:
    new_id = next_id("HISTORIAL", "ID_HISTORIAL")
    payload = {**data, "ID_HISTORIAL": new_id}
    sql = """
        INSERT INTO HISTORIAL
          (ID_HISTORIAL, ACCION, FECHA_EVENTO, USUARIO_ID_USUARIO, LIBRO_ID_LIBRO)
        VALUES
          (:ID_HISTORIAL, :ACCION, TO_DATE(:FECHA_EVENTO,'YYYY-MM-DD'), :USUARIO_ID_USUARIO, :LIBRO_ID_LIBRO)
    """
    execute(sql, payload)
    return new_id


def actualizar(id_historial: int, data: Dict[str, object]) -> None:
    payload = {**data, "ID": id_historial}
    sql = """
        UPDATE HISTORIAL
           SET ACCION = :ACCION,
               FECHA_EVENTO = TO_DATE(:FECHA_EVENTO,'YYYY-MM-DD'),
               USUARIO_ID_USUARIO = :USUARIO_ID_USUARIO,
               LIBRO_ID_LIBRO = :LIBRO_ID_LIBRO
         WHERE ID_HISTORIAL = :ID
    """
    execute(sql, payload)


def eliminar(id_historial: int) -> None:
    execute("DELETE FROM HISTORIAL WHERE ID_HISTORIAL = :ID", {"ID": id_historial})
