"""DAO helpers for EDIT_LIB or related tables with dynamic schema discovery."""

from __future__ import annotations

from typing import Dict, List, Optional

import oracledb

from .db import (
    any_column_like,
    execute,
    find_fk_to,
    first_existing_column,
    first_existing_table,
    find_sequence_for,
    get_col_datatype,
    next_pk_from_max,
    query_all,
    query_one,
    table_has_identity,
)


def _child_table() -> str:
    """Return the table used by the LibroEdit module (child table)."""

    return first_existing_table([
        "EDIT_LIB",
        "LIBRO_EDITORIAL",
        "LIBRO_EDICION",
        "LIBRO_EDIT",
    ])


def _parent_table() -> Optional[str]:
    try:
        return first_existing_table(["LIBRO_EDIT"])
    except RuntimeError:
        return None


def _pk(table: str) -> str:
    try:
        return first_existing_column(table, ["ID_EDIT_LIB", "ID_LIBRO_EDIT", "ID_EDIT", "ID"])
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


def _parent_pk() -> Optional[str]:
    table = _parent_table()
    if not table:
        return None
    try:
        return first_existing_column(table, ["ID_LIBRO_EDIT", "ID_EDIT_LIB", "ID_EDIT", "ID"])
    except RuntimeError:
        return "ROWID"


def _parent_libro_fk() -> Optional[str]:
    table = _parent_table()
    if not table:
        return None
    return find_fk_to(table, "LIBRO") or any_column_like(table, ["LIBRO", "ID_LIB"])


def _parent_editorial_fk() -> Optional[str]:
    table = _parent_table()
    if not table:
        return None
    try:
        return first_existing_column(table, ["ID_VAREDIT", "EDITORIAL_ID", "ID_EDITORIAL"])
    except RuntimeError:
        return find_fk_to(table, "EDITORIAL") or any_column_like(
            table, ["EDITOR", "VAREDIT", "EDIT"]
        )


def _parent_fecha_col() -> Optional[str]:
    table = _parent_table()
    if not table:
        return None
    return any_column_like(table, ["FECHA_EDICION", "FECHA", "FEC_EDI"])


def _find_parent(libro_id: int, editorial_id: int, fecha_iso: Optional[str]) -> Optional[int]:
    table = _parent_table()
    if not table:
        return None

    pk = _parent_pk()
    libro_fk = _parent_libro_fk()
    editorial_fk = _parent_editorial_fk()
    fecha_col = _parent_fecha_col()

    if not libro_fk or not editorial_fk:
        raise RuntimeError("No se pudieron identificar columnas en LIBRO_EDIT.")

    conditions = [f"{libro_fk} = :libro", f"{editorial_fk} = :editorial"]
    payload: Dict[str, object] = {"libro": int(libro_id), "editorial": int(editorial_id)}

    if fecha_col and fecha_iso:
        if get_col_datatype(table, fecha_col) == "DATE":
            conditions.append(f"TRUNC({fecha_col}) = TRUNC(TO_DATE(:fecha, 'YYYY-MM-DD'))")
        else:
            conditions.append(f"{fecha_col} = :fecha")
        payload["fecha"] = fecha_iso

    sql = (
        f"SELECT {pk} AS ID FROM {table} WHERE "
        + " AND ".join(conditions)
        + " FETCH FIRST 1 ROWS ONLY"
    )
    row = query_one(sql, payload)
    if not row or row.get("ID") is None:
        return None
    return int(row["ID"])


def _ensure_parent(libro_id: int, editorial_id: int, fecha_iso: Optional[str]) -> int:
    table = _parent_table()
    if not table:
        raise RuntimeError("La tabla LIBRO_EDIT no existe en el esquema.")

    existing = _find_parent(libro_id, editorial_id, fecha_iso)
    if existing is not None:
        return existing

    pk = _parent_pk()
    libro_fk = _parent_libro_fk()
    editorial_fk = _parent_editorial_fk()
    fecha_col = _parent_fecha_col()

    if not libro_fk or not editorial_fk:
        raise RuntimeError("No se pudieron identificar columnas de libro o editorial en LIBRO_EDIT.")

    columns: List[str] = []
    values: List[str] = []
    payload: Dict[str, object] = {"libro": int(libro_id), "editorial": int(editorial_id)}

    if pk and pk != "ROWID" and not table_has_identity(table, pk):
        sequence = find_sequence_for(table, pk)
        if sequence:
            rid = query_one(f"SELECT {sequence}.NEXTVAL AS ID FROM DUAL")
            payload["id"] = int(rid["ID"]) if rid and rid.get("ID") is not None else None
            columns.append(pk)
            values.append(":id")
        else:
            payload["id"] = int(next_pk_from_max(table, pk))
            columns.append(pk)
            values.append(":id")

    columns.extend([libro_fk, editorial_fk])
    values.extend([":libro", ":editorial"])

    if fecha_col and fecha_iso:
        if get_col_datatype(table, fecha_col) == "DATE":
            columns.append(fecha_col)
            values.append("TO_DATE(:fecha, 'YYYY-MM-DD')")
        else:
            columns.append(fecha_col)
            values.append(":fecha")
        payload["fecha"] = fecha_iso

    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)})"
    try:
        execute(sql, payload)
    except oracledb.IntegrityError as exc:  # type: ignore[attr-defined]
        if "ORA-00001" not in str(exc):
            raise
    result = _find_parent(libro_id, editorial_id, fecha_iso)
    if result is None:
        raise RuntimeError("No se pudo obtener ID de LIBRO_EDIT tras el insert")
    return result


def listar() -> List[Dict[str, object]]:
    table = _child_table()
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
    table = _child_table()
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
    fecha_iso = data.get("FECHA") or data.get("FECHA_EDICION")

    if libro_raw is None:
        raise ValueError("LIBRO_ID requerido para crear relación de edición.")
    libro_id = int(libro_raw)

    editorial_id: Optional[int]
    if editorial_raw in (None, ""):
        editorial_id = None
    else:
        editorial_id = int(editorial_raw)

    if libro_edit_fk:
        if editorial_id is None:
            raise ValueError(
                "EDITORIAL_ID requerido (ID_VAREDIT/ID_EDITORIAL/etc) para LIBRO_EDIT."
            )
        parent_id = _ensure_parent(libro_id, editorial_id, fecha_iso)
        try:
            parent_id = int(parent_id)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"LIBRO_EDIT_ID inválido: {parent_id!r}."
            ) from exc

        columns: List[str] = []
        values: List[str] = []
        payload: Dict[str, object] = {}

        pk = _pk(table)
        if pk != "ROWID" and not table_has_identity(table, pk):
            sequence = find_sequence_for(table, pk)
            if sequence:
                columns.append(pk)
                values.append(f"{sequence}.NEXTVAL")
            else:
                columns.append(pk)
                values.append(":pk")
                payload["pk"] = int(next_pk_from_max(table, pk))

        columns.append(libro_edit_fk)
        values.append(":parent")
        payload["parent"] = int(parent_id)

        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)})"
        execute(sql, payload)
        return

    columns: List[str] = []
    values: List[str] = []
    payload: Dict[str, object] = {}

    pk = _pk(table)
    if pk != "ROWID" and not table_has_identity(table, pk):
        sequence = find_sequence_for(table, pk)
        if sequence:
            columns.append(pk)
            values.append(f"{sequence}.NEXTVAL")
        else:
            columns.append(pk)
            values.append(":pk")
            payload["pk"] = int(next_pk_from_max(table, pk))

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
        if get_col_datatype(table, fecha_col) == "DATE":
            columns.append(fecha_col)
            values.append("TO_DATE(:fecha, 'YYYY-MM-DD')")
        else:
            columns.append(fecha_col)
            values.append(":fecha")
        payload["fecha"] = fecha_iso

    columns = list(dict.fromkeys(columns))
    values = values[: len(columns)]

    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)})"
    execute(sql, payload)
