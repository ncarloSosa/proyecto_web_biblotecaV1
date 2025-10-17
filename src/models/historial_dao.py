"""DAO helpers for HISTORIAL with metadata-driven column discovery."""

from __future__ import annotations

from typing import Dict, List

from .db import (
    any_column_like,
    find_fk_to,
    first_existing_column,
    query_all,
)


def _pk_hist() -> str:
    try:
        return first_existing_column("HISTORIAL", ["ID_HISTORIAL", "ID_HIST", "ID"])
    except RuntimeError:
        return "ROWID"


def _libro_fk() -> str:
    fk = find_fk_to("HISTORIAL", "LIBRO")
    if fk:
        return fk
    return any_column_like("HISTORIAL", ["LIBRO", "ID_LIB", "LIB"])


def _usuario_fk() -> str | None:
    fk = find_fk_to("HISTORIAL", "USUARIO")
    if fk:
        return fk
    for table in ("MIEMBRO", "CLIENTE"):
        fk = find_fk_to("HISTORIAL", table)
        if fk:
            return fk
    return any_column_like(
        "HISTORIAL",
        ["USUARIO", "ID_US", "MIEMBRO", "CLIENTE", "ID_CLI"],
    )


def listar() -> List[Dict[str, object]]:
    pk = _pk_hist()
    libro_fk = _libro_fk() or first_existing_column(
        "HISTORIAL", ["LIBRO_ID_LIBRO", "ID_LIBRO", "LIBRO"]
    )
    usuario_fk = _usuario_fk()
    usuario_sel = f"{usuario_fk} AS USUARIO_ID" if usuario_fk else "NULL AS USUARIO_ID"
    fecha = any_column_like("HISTORIAL", ["FECHA", "FEC", "CREA"])

    sql = f"""
        SELECT {pk} AS ID,
               {libro_fk} AS LIBRO_ID,
               {usuario_sel},
               ACCION,
               {fecha or 'NULL'} AS FECHA_EVENTO
          FROM HISTORIAL
         ORDER BY ID DESC
    """
    return query_all(sql)
