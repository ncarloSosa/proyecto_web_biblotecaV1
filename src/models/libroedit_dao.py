"""DAO helpers for EDIT_LIB with dynamic parent detection."""

from __future__ import annotations

from typing import Dict, List, Optional

import oracledb

from .db import first_existing_column, query_all, query_one, execute

TABLE_CHILD = "EDIT_LIB"
TABLE_PARENT = "LIBRO_EDIT"


def _find_sequence_like(prefix: str) -> Optional[str]:
    row = query_one(
        "SELECT sequence_name FROM user_sequences "
        "WHERE UPPER(sequence_name) LIKE :p FETCH FIRST 1 ROWS ONLY",
        {"p": f"{prefix.upper()}%SEQ"},
    )
    return row["SEQUENCE_NAME"] if row else None


def _pk(table: str = TABLE_CHILD) -> str:
    try:
        return first_existing_column(table, ["ID_EDIT_LIB", "ID_LIBRO_EDIT", "ID_EDIT", "ID"])
    except Exception:
        # fallback seguro cuando no se identifica PK numérica
        return "ROWID"


def _child_col(candidates: List[str]) -> Optional[str]:
    try:
        return first_existing_column(TABLE_CHILD, candidates)
    except Exception:
        return None


def _find_parent(libro_id: int, editorial_id: int, fecha_iso: Optional[str]) -> Optional[object]:
    conditions = ["LIBRO_ID_LIBRO = :lib", "EDITORIAL_ID = :edi"]
    payload: Dict[str, object] = {"lib": libro_id, "edi": editorial_id}
    if fecha_iso:
        conditions.append("TRUNC(FECHA_EDICION) = TRUNC(TO_DATE(:fec,'YYYY-MM-DD'))")
        payload["fec"] = fecha_iso
    sql = (
        "SELECT ID_VAREDIT AS ID FROM LIBRO_EDIT WHERE "
        + " AND ".join(conditions)
        + " FETCH FIRST 1 ROWS ONLY"
    )
    row = query_one(sql, payload)
    return row["ID"] if row else None


def _ensure_parent(libro_id: int, editorial_id: int, fecha_iso: Optional[str]) -> object:
    existing = _find_parent(libro_id, editorial_id, fecha_iso)
    if existing is not None:
        return existing

    seq = _find_sequence_like("LIBRO_EDIT")
    columns = ["LIBRO_ID_LIBRO", "EDITORIAL_ID"]
    values = [":lib", ":edi"]
    payload: Dict[str, object] = {"lib": libro_id, "edi": editorial_id}

    if fecha_iso:
        columns.append("FECHA_EDICION")
        values.append("TO_DATE(:fec,'YYYY-MM-DD')")
        payload["fec"] = fecha_iso

    if seq:
        columns.insert(0, "ID_VAREDIT")
        values.insert(0, f"{seq}.NEXTVAL")
        sql = f"INSERT INTO {TABLE_PARENT} ({', '.join(columns)}) VALUES ({', '.join(values)})"
        execute(sql, payload)
    else:
        new_id = (query_one(f"SELECT NVL(MAX(ID_VAREDIT),0)+1 ID FROM {TABLE_PARENT}") or {"ID": 1})["ID"]
        columns.insert(0, "ID_VAREDIT")
        values.insert(0, ":id")
        payload["id"] = new_id
        sql = f"INSERT INTO {TABLE_PARENT} ({', '.join(columns)}) VALUES ({', '.join(values)})"
        execute(sql, payload)

    try:
        return _find_parent(libro_id, editorial_id, fecha_iso)
    except Exception as exc:
        raise RuntimeError("No se pudo obtener ID de LIBRO_EDIT tras insert/select.") from exc


def listar() -> List[Dict[str, object]]:
    table = TABLE_CHILD
    pk = _pk(table)
    libro_edit_fk = _child_col(["LIBRO_EDIT_ID", "ID_LIBRO_EDIT", "LIBRO_EDIT"])
    libro_fk = _child_col(["LIBRO_ID_LIBRO", "ID_LIBRO", "LIBRO_ID"])
    editorial_fk = _child_col(["ID_VAREDIT", "EDITORIAL_ID", "ID_EDITORIAL"])
    fecha_col = _child_col(["FECHA", "FECHA_EDICION", "FEC_EDI"])

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
    table = TABLE_CHILD
    libro_edit_fk = _child_col(["LIBRO_EDIT_ID", "ID_LIBRO_EDIT", "LIBRO_EDIT"])
    libro_fk = _child_col(["LIBRO_ID_LIBRO", "ID_LIBRO", "LIBRO_ID"])
    editorial_fk = _child_col(["ID_VAREDIT", "EDITORIAL_ID", "ID_EDITORIAL"])
    fecha_col = _child_col(["FECHA", "FECHA_EDICION", "FEC_EDI"])

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

        columns: List[str] = []
        values: List[str] = []
        payload: Dict[str, object] = {}

        pk = _pk(table)
        if pk != "ROWID":
            sequence = _find_sequence_like(table)
            if sequence:
                columns.append(pk)
                values.append(f"{sequence}.NEXTVAL")
            else:
                new_id = (query_one(f"SELECT NVL(MAX({pk}),0)+1 ID FROM {table}") or {"ID": 1})["ID"]
                columns.append(pk)
                values.append(":pk")
                payload["pk"] = new_id

        columns.append(libro_edit_fk)
        values.append(":parent")
        payload["parent"] = parent_id

        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)})"
        execute(sql, payload)
        return

    columns = []
    values = []
    payload: Dict[str, object] = {}

    pk = _pk(table)
    if pk != "ROWID":
        sequence = _find_sequence_like(table)
        if sequence:
            columns.append(pk)
            values.append(f"{sequence}.NEXTVAL")
        else:
            new_id = (query_one(f"SELECT NVL(MAX({pk}),0)+1 ID FROM {table}") or {"ID": 1})["ID"]
            columns.append(pk)
            values.append(":pk")
            payload["pk"] = new_id

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
        values.append("TO_DATE(:fecha, 'YYYY-MM-DD')")
        payload["fecha"] = fecha_iso

    columns = list(dict.fromkeys(columns))
    values = values[: len(columns)]

    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)})"
    execute(sql, payload)
