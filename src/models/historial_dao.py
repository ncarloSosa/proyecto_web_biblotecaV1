"""DAO helpers for the HISTORIAL table."""

from __future__ import annotations

from typing import Dict, List, Optional

from .db import (
    any_column_like,
    execute,
    find_fk_to,
    first_existing_column,
    get_col_datatype,
    next_id,
    query_all,
    query_one,
)


def _libro_fk() -> str:
    fk = find_fk_to("HISTORIAL", "LIBRO")
    if not fk:
        fk = any_column_like("HISTORIAL", ["LIBRO", "ID_LIB"])
    if not fk:
        raise RuntimeError("No se encuentra columna de libro en HISTORIAL")
    return fk


def _usuario_fk() -> str:
    fk = find_fk_to("HISTORIAL", "USUARIO")
    if not fk:
        fk = any_column_like("HISTORIAL", ["USU", "ID_US"])
    if not fk:
        raise RuntimeError("No se encuentra columna de usuario en HISTORIAL")
    return fk


def listar() -> List[Dict[str, object]]:
    lf = _libro_fk()
    uf = _usuario_fk()
    accion_col = first_existing_column("HISTORIAL", ["ACCION", "TIP_MOV", "MOVIMIENTO"])
    fecha_col = first_existing_column("HISTORIAL", ["FECHA_EVENTO", "FECHA", "FEC_MOV", "FECHA_REGISTRO"])
    sql = f"""
        SELECT ID_HISTORIAL,
               {accion_col} AS ACCION,
               {fecha_col} AS FECHA_EVENTO,
               {uf} AS USUARIO_ID,
               {lf} AS LIBRO_ID
          FROM HISTORIAL
         ORDER BY {fecha_col} DESC
    """
    return query_all(sql)


def obtener(id_historial: int) -> Optional[Dict[str, object]]:
    lf = _libro_fk()
    uf = _usuario_fk()
    accion_col = first_existing_column("HISTORIAL", ["ACCION", "TIP_MOV", "MOVIMIENTO"])
    fecha_col = first_existing_column("HISTORIAL", ["FECHA_EVENTO", "FECHA", "FEC_MOV", "FECHA_REGISTRO"])
    sql = f"""
        SELECT ID_HISTORIAL,
               {accion_col} AS ACCION,
               {fecha_col} AS FECHA_EVENTO,
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
    accion_col = first_existing_column("HISTORIAL", ["ACCION", "TIP_MOV", "MOVIMIENTO"])
    fecha_col = first_existing_column("HISTORIAL", ["FECHA_EVENTO", "FECHA", "FEC_MOV", "FECHA_REGISTRO"])
    fecha_expr = (
        "TO_DATE(:FECHA_EVENTO,'YYYY-MM-DD')"
        if get_col_datatype("HISTORIAL", fecha_col) == "DATE"
        else ":FECHA_EVENTO"
    )
    payload = {**data, "ID_HISTORIAL": new_id}
    sql = f"""
        INSERT INTO HISTORIAL
          (ID_HISTORIAL, {accion_col}, {fecha_col}, {uf}, {lf})
        VALUES
          (:ID_HISTORIAL, :ACCION, {fecha_expr}, :USUARIO_ID, :LIBRO_ID)
    """
    execute(sql, payload)
    return new_id


def actualizar(id_historial: int, data: Dict[str, object]) -> None:
    lf = _libro_fk()
    uf = _usuario_fk()
    accion_col = first_existing_column("HISTORIAL", ["ACCION", "TIP_MOV", "MOVIMIENTO"])
    fecha_col = first_existing_column("HISTORIAL", ["FECHA_EVENTO", "FECHA", "FEC_MOV", "FECHA_REGISTRO"])
    fecha_expr = (
        "TO_DATE(:FECHA_EVENTO,'YYYY-MM-DD')"
        if get_col_datatype("HISTORIAL", fecha_col) == "DATE"
        else ":FECHA_EVENTO"
    )
    payload = {**data, "ID": id_historial}
    sql = f"""
        UPDATE HISTORIAL
           SET {accion_col} = :ACCION,
               {fecha_col} = {fecha_expr},
               {uf} = :USUARIO_ID,
               {lf} = :LIBRO_ID
         WHERE ID_HISTORIAL = :ID
    """
    execute(sql, payload)


def eliminar(id_historial: int) -> None:
    execute("DELETE FROM HISTORIAL WHERE ID_HISTORIAL = :ID", {"ID": id_historial})
