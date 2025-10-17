"""DAO helpers for book-editorial relations."""
from __future__ import annotations

from typing import Dict, List, Optional

from .db import (
    any_column_like,
    column_exists,
    execute,
    first_existing_column,
    first_existing_table,
    find_fk_to,
    get_col_datatype,
    next_id,
    query_all,
    query_one,
)


def _table() -> str:
    return first_existing_table(["EDIT_LIB", "LIBRO_EDIT", "LIBRO_EDITORIAL", "LIBRO_EDICION"])


def _pk() -> str:
    table = _table()
    try:
        return first_existing_column(table, ["ID_EDIT_LIB", "ID_LIBRO_EDIT", "ID_EDIT", "ID"])
    except RuntimeError:
        candidate = any_column_like(table, ["ID"])
        if candidate:
            return candidate
    raise RuntimeError(f"No se encontró columna identificadora en {table}")


def _libro_fk() -> str:
    table = _table()
    fk = find_fk_to(table, "LIBRO") or any_column_like(table, ["LIBRO", "ID_LIB"])
    if not fk:
        raise RuntimeError(f"No se encontró columna de libro en {table}")
    return fk


def _editorial_fk() -> str:
    table = _table()
    fk = find_fk_to(table, "EDITORIAL") or any_column_like(table, ["EDIT", "EDITOR", "VAREDIT"])
    if not fk:
        raise RuntimeError(f"No se encontró columna de editorial en {table}")
    return fk


def _date_col() -> Optional[str]:
    table = _table()
    for candidate in ["FECHA_EDICION", "FECHA_REGISTRO", "FECHA"]:
        if column_exists(table, candidate):
            return candidate
    return None


def _date_expr(column: str) -> str:
    table = _table()
    datatype = get_col_datatype(table, column)
    if datatype == "DATE":
        return "TO_DATE(:FECHA,'YYYY-MM-DD')"
    return ":FECHA"


def listar() -> List[Dict[str, object]]:
    table = _table()
    pk = _pk()
    lf = _libro_fk()
    ef = _editorial_fk()
    dc = _date_col()
    extra = f", {dc} AS FECHA" if dc else ""
    sql = f"""
        SELECT {pk} AS ID,
               {lf} AS LIBRO_ID,
               {ef} AS EDITORIAL_ID{extra}
          FROM {table}
         ORDER BY {pk} DESC
    """
    return query_all(sql)


def obtener(id_edit_lib: int) -> Optional[Dict[str, object]]:
    table = _table()
    pk = _pk()
    lf = _libro_fk()
    ef = _editorial_fk()
    dc = _date_col()
    extra = f", {dc} AS FECHA" if dc else ""
    sql = f"""
        SELECT {pk} AS ID,
               {lf} AS LIBRO_ID,
               {ef} AS EDITORIAL_ID{extra}
          FROM {table}
         WHERE {pk} = :ID
    """
    return query_one(sql, {"ID": id_edit_lib})


def crear(data: Dict[str, object]) -> int:
    table = _table()
    pk = _pk()
    lf = _libro_fk()
    ef = _editorial_fk()
    dc = _date_col()
    new_id = next_id(table, pk)

    columns = [pk, lf, ef]
    values = [f":{pk}", ":LIBRO_ID", ":EDITORIAL_ID"]
    payload: Dict[str, object] = {
        pk: new_id,
        "LIBRO_ID": data["LIBRO_ID"],
        "EDITORIAL_ID": data["EDITORIAL_ID"],
    }

    if dc:
        columns.append(dc)
        values.append(_date_expr(dc))
        payload["FECHA"] = data.get("FECHA")
    else:
        payload.pop("FECHA", None)

    sql = f"""
        INSERT INTO {table} ({', '.join(columns)})
        VALUES ({', '.join(values)})
    """
    execute(sql, payload)
    return new_id


def actualizar(id_edit_lib: int, data: Dict[str, object]) -> None:
    table = _table()
    pk = _pk()
    lf = _libro_fk()
    ef = _editorial_fk()
    dc = _date_col()

    payload: Dict[str, object] = {
        "ID": id_edit_lib,
        "LIBRO_ID": data["LIBRO_ID"],
        "EDITORIAL_ID": data["EDITORIAL_ID"],
    }

    set_parts = [f"{lf} = :LIBRO_ID", f"{ef} = :EDITORIAL_ID"]
    if dc:
        set_parts.append(f"{dc} = {_date_expr(dc)}")
        payload["FECHA"] = data.get("FECHA")
    else:
        payload.pop("FECHA", None)

    sql = f"""
        UPDATE {table}
           SET {', '.join(set_parts)}
         WHERE {pk} = :ID
    """
    execute(sql, payload)


def eliminar(id_edit_lib: int) -> None:
    table = _table()
    pk = _pk()
    execute(f"DELETE FROM {table} WHERE {pk} = :ID", {"ID": id_edit_lib})
