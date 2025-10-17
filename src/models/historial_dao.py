"""DAO helpers for the HISTORIAL table."""

from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, first_existing_column, next_id, query_all, query_one


def _libro_fk() -> str:
    return first_existing_column("HISTORIAL", ["LIBRO_ID_LIBRO", "ID_LIBRO"])


def _usuario_fk() -> str:
    return first_existing_column("HISTORIAL", ["USUARIO_ID_USUARIO", "ID_USUARIO"])


def listar() -> List[Dict[str, object]]:
    lf = _libro_fk()
    uf = _usuario_fk()
    sql = f"""
        SELECT ID_HISTORIAL,
               ACCION,
               FECHA_EVENTO,
               {uf} AS USUARIO_ID,
               {lf} AS LIBRO_ID
          FROM HISTORIAL
         ORDER BY FECHA_EVENTO DESC
    """
    return query_all(sql)


def obtener(id_historial: int) -> Optional[Dict[str, object]]:
    lf = _libro_fk()
    uf = _usuario_fk()
    sql = f"""
        SELECT ID_HISTORIAL,
               ACCION,
               FECHA_EVENTO,
               {uf} AS USUARIO_ID,
               {lf} AS LIBRO_ID
          FROM HISTORIAL
         WHERE ID_HISTORIAL = :ID
    """
    return query_one(sql, {"ID": id_historial})


def crear(data: Dict[str, object]) -> int:
    lf = _libro_fk()
    uf = _usuario_fk()
    new_id = next_id("HISTORIAL", "ID_HISTORIAL")
    payload = {**data, "ID_HISTORIAL": new_id}
    sql = f"""
        INSERT INTO HISTORIAL
          (ID_HISTORIAL, ACCION, FECHA_EVENTO, {uf}, {lf})
        VALUES
          (:ID_HISTORIAL, :ACCION, TO_DATE(:FECHA_EVENTO,'YYYY-MM-DD'), :USUARIO_ID, :LIBRO_ID)
    """
    execute(sql, payload)
    return new_id


def actualizar(id_historial: int, data: Dict[str, object]) -> None:
    lf = _libro_fk()
    uf = _usuario_fk()
    payload = {**data, "ID": id_historial}
    sql = f"""
        UPDATE HISTORIAL
           SET ACCION = :ACCION,
               FECHA_EVENTO = TO_DATE(:FECHA_EVENTO,'YYYY-MM-DD'),
               {uf} = :USUARIO_ID,
               {lf} = :LIBRO_ID
         WHERE ID_HISTORIAL = :ID
    """
    execute(sql, payload)


def eliminar(id_historial: int) -> None:
    execute("DELETE FROM HISTORIAL WHERE ID_HISTORIAL = :ID", {"ID": id_historial})
