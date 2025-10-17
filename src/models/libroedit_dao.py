"""DAO helpers for EDIT_LIB or related tables with dynamic schema discovery."""

from __future__ import annotations

from typing import Dict, List, Optional

from .db import (
    any_column_like,
    execute,
    first_existing_column,
    first_existing_table,
    find_fk_to,
    get_col_datatype,
    query_all,
    query_one,
    table_exists,
)


def _table() -> str:
    return first_existing_table(
        ["EDIT_LIB", "LIBRO_EDIT", "LIBRO_EDITORIAL", "LIBRO_EDICION"]
    )


def _pk(table: str) -> str:
    try:
        return first_existing_column(
            table, ["ID_EDIT_LIB", "ID_LIBRO_EDIT", "ID_EDIT", "ID"]
        )
    except RuntimeError:
        return "ROWID"


def _libro_fk_in(table: str) -> Optional[str]:
    return find_fk_to(table, "LIBRO") or any_column_like(table, ["LIBRO", "ID_LIB"])


def _editorial_fk_in(table: str) -> Optional[str]:
    try:
        return first_existing_column(table, ["ID_VAREDIT", "EDITORIAL_ID", "ID_EDITORIAL"])
    except RuntimeError:
        return find_fk_to(table, "EDITORIAL") or any_column_like(
            table, ["EDITOR", "VAREDIT", "EDIT"]
        )


def _fecha_col_in(table: str) -> Optional[str]:
    return any_column_like(table, ["FECHA_EDICION", "FECHA", "FEC_EDI"])


def _libro_edit_fk_in(table: str) -> Optional[str]:
    return find_fk_to(table, "LIBRO_EDIT") or any_column_like(
        table, ["LIBRO_EDIT", "ID_LIBRO_EDIT", "EDIT_ID"]
    )


def _libro_fk_in_le() -> Optional[str]:
    table = "LIBRO_EDIT"
    if not table_exists(table):
        return None
    return find_fk_to(table, "LIBRO") or any_column_like(table, ["LIBRO", "ID_LIB"])


def _editorial_fk_in_le() -> Optional[str]:
    table = "LIBRO_EDIT"
    if not table_exists(table):
        return None
    try:
        return first_existing_column(table, ["ID_VAREDIT", "EDITORIAL_ID", "ID_EDITORIAL"])
    except RuntimeError:
        return find_fk_to(table, "EDITORIAL") or any_column_like(
            table, ["EDITOR", "VAREDIT", "EDIT"]
        )


def _fecha_col_in_le() -> Optional[str]:
    table = "LIBRO_EDIT"
    if not table_exists(table):
        return None
    return any_column_like(table, ["FECHA_EDICION", "FECHA", "FEC_EDI"])


def _ensure_libro_edit(libro_id: int, editorial_id: int, fecha_iso: Optional[str]) -> int:
    table = first_existing_table(["LIBRO_EDIT"])
    pk = _pk(table)
    libro_fk = _libro_fk_in(table)
    editorial_fk = _editorial_fk_in(table)
    fecha_col = _fecha_col_in(table)

    if not libro_fk or not editorial_fk:
        raise RuntimeError("No se pudieron identificar las columnas de LIBRO_EDIT.")

    conditions = [f"{libro_fk} = :libro", f"{editorial_fk} = :editorial"]
    payload: Dict[str, object] = {"libro": libro_id, "editorial": editorial_id}

    if fecha_col and fecha_iso:
        if get_col_datatype(table, fecha_col) == "DATE":
            conditions.append(f"{fecha_col} = TO_DATE(:fecha, 'YYYY-MM-DD')")
        else:
            conditions.append(f"{fecha_col} = :fecha")
        payload["fecha"] = fecha_iso

    sql_find = (
        f"SELECT {pk} AS ID FROM {table} WHERE "
        + " AND ".join(conditions)
        + " FETCH FIRST 1 ROWS ONLY"
    )
    row = query_one(sql_find, payload)
    if row:
        return int(row["ID"])

    cols = [libro_fk, editorial_fk]
    vals = [":libro", ":editorial"]
    payload_insert = {"libro": libro_id, "editorial": editorial_id}

    if fecha_col and fecha_iso:
        cols.append(fecha_col)
        if get_col_datatype(table, fecha_col) == "DATE":
            vals.append("TO_DATE(:fecha, 'YYYY-MM-DD')")
        else:
            vals.append(":fecha")
        payload_insert["fecha"] = fecha_iso

    sql_insert = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(vals)})"
    execute(sql_insert, payload_insert)

    row = query_one(sql_find, payload)
    if not row:
        raise RuntimeError("No se pudo obtener ID de LIBRO_EDIT tras insertar")
    return int(row["ID"])


def listar() -> List[Dict[str, object]]:
    table = _table()
    pk = _pk(table)
    libro_edit_fk = _libro_edit_fk_in(table)
    libro_fk = _libro_fk_in(table)
    editorial_fk = _editorial_fk_in(table)
    fecha_col = _fecha_col_in(table)

    select_parts = [f"{pk} AS ID"]
    if libro_edit_fk:
        select_parts.append(f"{libro_edit_fk} AS LIBRO_EDIT_ID")
    if libro_fk:
        select_parts.append(f"{libro_fk} AS LIBRO_ID")
    if editorial_fk:
        select_parts.append(f"{editorial_fk} AS EDITORIAL_ID")
    if fecha_col:
        select_parts.append(f"{fecha_col} AS FECHA")

    sql = f"SELECT {', '.join(select_parts)} FROM {table} ORDER BY {pk} DESC"
    return query_all(sql)


def crear(data: Dict[str, object]) -> None:
    table = _table()
    libro_edit_fk = _libro_edit_fk_in(table)
    libro_fk = _libro_fk_in(table)
    editorial_fk = _editorial_fk_in(table)
    fecha_col = _fecha_col_in(table)

    libro_raw = data.get("LIBRO_ID") or data.get("ID_LIBRO")
    editorial_raw = (
        data.get("EDITORIAL_ID")
        or data.get("ID_VAREDIT")
        or data.get("ID_EDITORIAL")
        or data.get("VAREDIT_ID")
        or data.get("EDITORIAL")
    )
    if libro_raw is None:
        raise ValueError("LIBRO_ID requerido para crear relación de edición.")
    libro_id = int(libro_raw)
    if editorial_raw in (None, ""):
        editorial_id = None
    else:
        editorial_id = int(editorial_raw)
    fecha_iso = data.get("FECHA")

    if libro_edit_fk:
        if editorial_id is None:
            raise ValueError(
                "EDITORIAL_ID requerido (ID_VAREDIT/ID_EDITORIAL/etc) para LIBRO_EDIT."
            )
        libro_edit_id = _ensure_libro_edit(libro_id, editorial_id, fecha_iso)
        try:
            libro_edit_id = int(libro_edit_id)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"LIBRO_EDIT_ID inválido: {libro_edit_id!r}."
            ) from exc
        sql = f"INSERT INTO {table} ({libro_edit_fk}) VALUES (:libro_edit_id)"
        execute(sql, {"libro_edit_id": libro_edit_id})
        return

    columns: list[str] = []
    values: list[str] = []
    payload: Dict[str, object] = {}

    if libro_fk:
        columns.append(libro_fk)
        values.append(":libro")
        payload["libro"] = libro_id
    if editorial_fk:
        if editorial_id is None:
            raise ValueError("EDITORIAL_ID requerido para EDIT_LIB.")
        columns.append(editorial_fk)
        values.append(":editorial")
        payload["editorial"] = editorial_id
    if fecha_col and fecha_iso:
        columns.append(fecha_col)
        if get_col_datatype(table, fecha_col) == "DATE":
            values.append("TO_DATE(:fecha, 'YYYY-MM-DD')")
        else:
            values.append(":fecha")
        payload["fecha"] = fecha_iso

    if not columns:
        raise RuntimeError("No hay columnas para insertar en tabla de libro/edición")

    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)})"
    execute(sql, payload)
