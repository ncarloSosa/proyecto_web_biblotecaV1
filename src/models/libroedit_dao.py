"""DAO helpers for EDIT_LIB."""

from __future__ import annotations

from typing import Dict, List, Optional

from .db import (
    column_exists,
    execute,
    first_existing_column,
    next_id,
    query_all,
    query_one,
)


def _pk() -> str:
    return first_existing_column("EDIT_LIB", ["ID_EDIT_LIB", "ID_LIBRO_EDIT", "ID_EDIT"])


def _libro_fk() -> str:
    return first_existing_column("EDIT_LIB", ["LIBRO_ID_LIBRO", "ID_LIBRO"])


def _editorial_fk() -> str:
    return first_existing_column("EDIT_LIB", ["ID_EDITORIAL", "ID_VAREDIT", "NUM_EDITORIAL"])


def _date_col() -> Optional[str]:
    for candidate in ["FECHA_EDICION", "FECHA_REGISTRO", "FECHA"]:
        if column_exists("EDIT_LIB", candidate):
            return candidate
    return None


def listar() -> List[Dict[str, object]]:
    pk = _pk()
    lf = _libro_fk()
    ef = _editorial_fk()
    dc = _date_col()
    extra = f", {dc} AS FECHA" if dc else ""
    sql = f"""
        SELECT {pk} AS ID,
               {lf} AS LIBRO_ID,
               {ef} AS EDITORIAL_ID{extra}
          FROM EDIT_LIB
         ORDER BY {pk} DESC
    """
    return query_all(sql)


def obtener(id_edit_lib: int) -> Optional[Dict[str, object]]:
    pk = _pk()
    lf = _libro_fk()
    ef = _editorial_fk()
    dc = _date_col()
    extra = f", {dc} AS FECHA" if dc else ""
    sql = f"""
        SELECT {pk} AS ID,
               {lf} AS LIBRO_ID,
               {ef} AS EDITORIAL_ID{extra}
          FROM EDIT_LIB
         WHERE {pk} = :ID
    """
    return query_one(sql, {"ID": id_edit_lib})


def crear(data: Dict[str, object]) -> int:
    pk = _pk()
    lf = _libro_fk()
    ef = _editorial_fk()
    dc = _date_col()
    new_id = next_id("EDIT_LIB", pk)
    payload = {**data, pk: new_id}
    if dc is None:
        payload.pop("FECHA", None)
    date_fragment = (
        f"CASE WHEN :FECHA IS NOT NULL THEN TO_DATE(:FECHA,'YYYY-MM-DD') ELSE NULL END"
        if dc
        else "NULL"
    )
    sql = f"""
        INSERT INTO EDIT_LIB (
            {pk},
            {lf},
            {ef}{', ' + dc if dc else ''}
        ) VALUES (
            :{pk},
            :LIBRO_ID,
            :EDITORIAL_ID{', ' + date_fragment if dc else ''}
        )
    """
    execute(sql, payload)
    return new_id


def actualizar(id_edit_lib: int, data: Dict[str, object]) -> None:
    pk = _pk()
    lf = _libro_fk()
    ef = _editorial_fk()
    dc = _date_col()
    payload = {**data, "ID": id_edit_lib}
    if dc is None:
        payload.pop("FECHA", None)
    date_set = (
        f", {dc} = CASE WHEN :FECHA IS NOT NULL THEN TO_DATE(:FECHA,'YYYY-MM-DD') ELSE NULL END"
        if dc
        else ""
    )
    sql = f"""
        UPDATE EDIT_LIB
           SET {lf} = :LIBRO_ID,
               {ef} = :EDITORIAL_ID{date_set}
         WHERE {pk} = :ID
    """
    execute(sql, payload)


def eliminar(id_edit_lib: int) -> None:
    pk = _pk()
    execute(f"DELETE FROM EDIT_LIB WHERE {pk} = :ID", {"ID": id_edit_lib})
