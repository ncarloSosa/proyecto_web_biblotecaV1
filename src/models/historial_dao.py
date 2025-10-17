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


def _libro_fk(required: bool = True) -> Optional[str]:
    fk = find_fk_to("HISTORIAL", "LIBRO")
    if not fk:
        fk = any_column_like("HISTORIAL", ["LIBRO", "ID_LIB"])
    if not fk and required:
        raise RuntimeError("No se encuentra columna de libro en HISTORIAL")
    return fk


def _usuario_fk(optional: bool = False) -> Optional[str]:
    fk = (
        find_fk_to("HISTORIAL", "USUARIO")
        or find_fk_to("HISTORIAL", "MIEMBRO")
        or find_fk_to("HISTORIAL", "CLIENTE")
    )
    if not fk:
        fk = (
            any_column_like("HISTORIAL", ["USUARIO", "ID_US"])
            or any_column_like("HISTORIAL", ["MIEMBRO", "ID_MIE"])
            or any_column_like("HISTORIAL", ["CLIENTE", "ID_CLI"])
        )
    if not fk and not optional:
        raise RuntimeError("No se encuentra columna de usuario en HISTORIAL")
    return fk


def _fecha_expr(column: str) -> str:
    datatype = get_col_datatype("HISTORIAL", column)
    return "TO_DATE(:FECHA_EVENTO,'YYYY-MM-DD')" if datatype == "DATE" else ":FECHA_EVENTO"


def listar() -> List[Dict[str, object]]:
    lf = _libro_fk(required=False) or first_existing_column(
        "HISTORIAL", ["LIBRO_ID_LIBRO", "ID_LIBRO"]
    )
    uf = _usuario_fk(optional=True)
    accion_col = first_existing_column(
        "HISTORIAL", ["ACCION", "TIP_MOV", "MOVIMIENTO"]
    )
    try:
        fecha_col = first_existing_column(
            "HISTORIAL", ["FECHA_EVENTO", "FECHA", "FEC_MOV", "FECHA_REGISTRO"]
        )
    except RuntimeError:
        fecha_col = any_column_like("HISTORIAL", ["FECHA", "FEC", "CREA"])

    uf_select = f"{uf} AS USUARIO_ID" if uf else "NULL AS USUARIO_ID"
    fecha_select = fecha_col or "NULL"

    sql = f"""
        SELECT ID_HISTORIAL,
               {accion_col} AS ACCION,
               {fecha_select} AS FECHA_EVENTO,
               {uf_select},
               {lf} AS LIBRO_ID
          FROM HISTORIAL
         ORDER BY ID_HISTORIAL DESC
    """
    return query_all(sql)


def obtener(id_historial: int) -> Optional[Dict[str, object]]:
    lf = _libro_fk()
    uf = _usuario_fk(optional=True)
    accion_col = first_existing_column(
        "HISTORIAL", ["ACCION", "TIP_MOV", "MOVIMIENTO"]
    )
    try:
        fecha_col = first_existing_column(
            "HISTORIAL", ["FECHA_EVENTO", "FECHA", "FEC_MOV", "FECHA_REGISTRO"]
        )
    except RuntimeError:
        fecha_col = any_column_like("HISTORIAL", ["FECHA", "FEC", "CREA"])

    uf_select = f", {uf} AS USUARIO_ID" if uf else ", NULL AS USUARIO_ID"
    fecha_select = fecha_col or "NULL"

    sql = f"""
        SELECT ID_HISTORIAL,
               {accion_col} AS ACCION,
               {fecha_select} AS FECHA_EVENTO,
               {lf} AS LIBRO_ID{uf_select}
          FROM HISTORIAL
         WHERE ID_HISTORIAL = :ID
    """
    return query_one(sql, {"ID": id_historial})


def crear(data: Dict[str, object]) -> int:
    lf = _libro_fk()
    uf = _usuario_fk(optional=True)
    new_id = next_id("HISTORIAL", "ID_HISTORIAL")
    accion_col = first_existing_column(
        "HISTORIAL", ["ACCION", "TIP_MOV", "MOVIMIENTO"]
    )
    try:
        fecha_col = first_existing_column(
            "HISTORIAL", ["FECHA_EVENTO", "FECHA", "FEC_MOV", "FECHA_REGISTRO"]
        )
    except RuntimeError:
        fecha_col = any_column_like("HISTORIAL", ["FECHA", "FEC", "CREA"])

    columns = ["ID_HISTORIAL", accion_col, lf]
    values = [":ID_HISTORIAL", ":ACCION", ":LIBRO_ID"]
    payload: Dict[str, object] = {
        "ID_HISTORIAL": new_id,
        "ACCION": data["ACCION"],
        "LIBRO_ID": data["LIBRO_ID"],
    }

    if uf:
        columns.append(uf)
        values.append(":USUARIO_ID")
        payload["USUARIO_ID"] = data.get("USUARIO_ID")

    if fecha_col:
        columns.append(fecha_col)
        values.append(_fecha_expr(fecha_col))
        payload["FECHA_EVENTO"] = data.get("FECHA_EVENTO")

    sql = f"""
        INSERT INTO HISTORIAL ({', '.join(columns)})
        VALUES ({', '.join(values)})
    """
    execute(sql, payload)
    return new_id


def actualizar(id_historial: int, data: Dict[str, object]) -> None:
    lf = _libro_fk()
    uf = _usuario_fk(optional=True)
    accion_col = first_existing_column(
        "HISTORIAL", ["ACCION", "TIP_MOV", "MOVIMIENTO"]
    )
    try:
        fecha_col = first_existing_column(
            "HISTORIAL", ["FECHA_EVENTO", "FECHA", "FEC_MOV", "FECHA_REGISTRO"]
        )
    except RuntimeError:
        fecha_col = any_column_like("HISTORIAL", ["FECHA", "FEC", "CREA"])

    set_parts = [f"{accion_col} = :ACCION", f"{lf} = :LIBRO_ID"]
    payload: Dict[str, object] = {
        "ID": id_historial,
        "ACCION": data["ACCION"],
        "LIBRO_ID": data["LIBRO_ID"],
    }

    if uf:
        set_parts.append(f"{uf} = :USUARIO_ID")
        payload["USUARIO_ID"] = data.get("USUARIO_ID")

    if fecha_col:
        set_parts.append(f"{fecha_col} = {_fecha_expr(fecha_col)}")
        payload["FECHA_EVENTO"] = data.get("FECHA_EVENTO")

    sql = f"""
        UPDATE HISTORIAL
           SET {', '.join(set_parts)}
         WHERE ID_HISTORIAL = :ID
    """
    execute(sql, payload)


def eliminar(id_historial: int) -> None:
    execute("DELETE FROM HISTORIAL WHERE ID_HISTORIAL = :ID", {"ID": id_historial})
