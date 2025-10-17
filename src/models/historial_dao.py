"""DAO helpers for HISTORIAL with metadata-driven column discovery."""

from __future__ import annotations

from collections import OrderedDict
from typing import Dict, List

from .db import (
    any_column_like,
    execute,
    find_fk_to,
    first_existing_column,
    get_col_datatype,
    query_all,
    query_one,
    table_has_identity,
    find_sequence_for,
    next_pk_from_max,
)


def _table() -> str:
    return "HISTORIAL"


def _pk() -> str:
    try:
        return first_existing_column(_table(), ["ID_HISTORIAL", "ID_HIST", "ID"])
    except RuntimeError:
        return "ROWID"


def _libro_fk() -> str:
    table = _table()
    fk = find_fk_to(table, "LIBRO") or any_column_like(
        table, ["LIBRO", "ID_LIB", "LIB"]
    )
    if fk:
        return fk
    return first_existing_column(table, ["LIBRO_ID_LIBRO", "ID_LIBRO", "LIBRO"])


def _usuario_fk() -> str | None:
    table = _table()
    fk = find_fk_to(table, "USUARIO")
    if fk:
        return fk
    for ref in ("MIEMBRO", "CLIENTE"):
        fk = find_fk_to(table, ref)
        if fk:
            return fk
    return any_column_like(
        table,
        ["USUARIO", "ID_US", "MIEMBRO", "CLIENTE", "ID_CLI"],
    )


def _accion_col() -> str | None:
    table = _table()
    for candidate in [
        "ACCION",
        "ACCION_REALIZADA",
        "EVENTO",
        "OPERACION",
        "DESCRIPCION",
        "DETALLE",
    ]:
        try:
            return first_existing_column(table, [candidate])
        except RuntimeError:
            continue
    return None


def _fecha_col() -> str | None:
    return any_column_like(_table(), ["FECHA", "FEC", "CREA"])


def _format_date_select(column: str | None) -> str:
    if not column:
        return "NULL"
    if get_col_datatype(_table(), column) == "DATE":
        return f"TO_CHAR({column}, 'YYYY-MM-DD HH24:MI:SS')"
    return column


def listar() -> List[Dict[str, object]]:
    table = _table()
    pk = _pk()
    libro_fk = _libro_fk()
    usuario_fk = _usuario_fk()
    accion_col = _accion_col() or "NULL"
    fecha_col = _format_date_select(_fecha_col())
    usuario_sel = f"{usuario_fk} AS USUARIO_ID" if usuario_fk else "NULL AS USUARIO_ID"

    sql = f"""
        SELECT {pk} AS ID_HISTORIAL,
               {libro_fk} AS LIBRO_ID,
               {usuario_sel},
               {accion_col} AS ACCION,
               {fecha_col} AS FECHA_EVENTO
          FROM {table}
         ORDER BY ID_HISTORIAL DESC
    """
    return query_all(sql)


def obtener(id_historial: int) -> Dict[str, object] | None:
    table = _table()
    pk = _pk()
    libro_fk = _libro_fk()
    usuario_fk = _usuario_fk()
    accion_col = _accion_col() or "NULL"
    fecha_col = _format_date_select(_fecha_col())
    usuario_sel = f"{usuario_fk} AS USUARIO_ID" if usuario_fk else "NULL AS USUARIO_ID"

    sql = f"""
        SELECT {pk} AS ID_HISTORIAL,
               {libro_fk} AS LIBRO_ID,
               {usuario_sel},
               {accion_col} AS ACCION,
               {fecha_col} AS FECHA_EVENTO
          FROM {table}
         WHERE {pk} = :id
    """
    return query_one(sql, {"id": int(id_historial)})


def crear(data: Dict[str, object]) -> None:
    table = _table()
    pk = _pk()
    if pk == "ROWID":
        raise RuntimeError(
            "La tabla HISTORIAL requiere una columna de PK numérica para insertar registros."
        )

    libro_fk = _libro_fk()
    usuario_fk = _usuario_fk()
    if not libro_fk or not usuario_fk:
        raise RuntimeError("Columnas de libro o usuario no detectadas en HISTORIAL.")

    accion_col = _accion_col()
    fecha_col = _fecha_col()

    columns: OrderedDict[str, str] = OrderedDict()
    payload: Dict[str, object] = {}

    if not table_has_identity(table, pk):
        sequence = find_sequence_for(table, pk)
        if sequence:
            columns[pk] = f"{sequence}.NEXTVAL"
        else:
            columns[pk] = ":pk"
            payload["pk"] = int(next_pk_from_max(table, pk))

    columns[libro_fk] = ":libro"
    columns[usuario_fk] = ":usuario"
    payload["libro"] = (
        data.get("LIBRO_ID")
        or data.get("ID_LIBRO")
        or data.get("LIBRO_ID_LIBRO")
    )
    payload["usuario"] = (
        data.get("USUARIO_ID")
        or data.get("ID_USUARIO")
        or data.get("USUARIO_ID_USUARIO")
    )

    if accion_col:
        columns[accion_col] = ":accion"
        payload["accion"] = (
            data.get("ACCION")
            or data.get("DETALLE")
            or data.get("DESCRIPCION")
            or "ACTUALIZACION"
        )

    if fecha_col:
        if get_col_datatype(table, fecha_col) == "DATE":
            columns[fecha_col] = "SYSDATE"
        else:
            fecha_val = data.get("FECHA_EVENTO")
            if fecha_val:
                columns[fecha_col] = ":fecha"
                payload["fecha"] = fecha_val

    col_list = ", ".join(columns.keys())
    val_list = ", ".join(columns.values())
    sql = f"INSERT INTO {table} ({col_list}) VALUES ({val_list})"
    execute(sql, payload)
