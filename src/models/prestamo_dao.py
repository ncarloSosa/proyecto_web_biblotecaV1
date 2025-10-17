"""DAO helpers for PRESTAMO with dynamic schema detection."""

from __future__ import annotations

from typing import Dict, List

from .db import (
    any_column_like,
    execute,
    find_sequence_for,
    first_existing_column,
    get_col_datatype,
    query_all,
    table_has_identity,
    next_pk_from_max,
)


def _fec_prestamo_col() -> str | None:
    try:
        return first_existing_column(
            "PRESTAMO", ["FECHA_PRESTAMO", "FECHA_PRESTADO", "FEC_PRESTAMO"]
        )
    except RuntimeError:
        return any_column_like("PRESTAMO", ["FEC_PRE", "FECHA_PRES"])


def _fec_cad_col() -> str | None:
    try:
        return first_existing_column(
            "PRESTAMO",
            [
                "FECHA_CADUCIDAD",
                "FEC_CADUCIDAD",
                "FECHA_VENCIMIENTO",
                "FEC_VENCIMIENTO",
                "FECHA_DEVOLUCION",
                "FEC_DEVOLUCION",
            ],
        )
    except RuntimeError:
        return any_column_like("PRESTAMO", ["CADUC", "VENC", "DEVOL"])


def _to_date_expr(table: str, column: str | None, bind: str) -> str | None:
    if not column:
        return None
    return (
        f"TO_DATE({bind}, 'YYYY-MM-DD')"
        if get_col_datatype(table, column) == "DATE"
        else bind
    )


def listar() -> List[Dict[str, object]]:
    pk = first_existing_column("PRESTAMO", ["ID_PRESTAMO", "ID"])
    libro_fk = first_existing_column("PRESTAMO", ["LIBRO_ID_LIBRO", "ID_LIBRO", "LIBRO"])
    usuario_fk = first_existing_column(
        "PRESTAMO", ["USUARIO_ID_USUARIO", "ID_USUARIO", "USUARIO"]
    )
    fp = _fec_prestamo_col()
    fc = _fec_cad_col()

    fp_sel = f", {fp} AS FECHA_PRESTAMO" if fp else ", NULL AS FECHA_PRESTAMO"
    fc_sel = f", {fc} AS FECHA_CADUCIDAD" if fc else ", NULL AS FECHA_CADUCIDAD"

    sql = f"""
        SELECT {pk} AS ID_PRESTAMO,
               {libro_fk} AS LIBRO_ID,
               {usuario_fk} AS USUARIO_ID
               {fp_sel}
               {fc_sel},
               ESTADO
          FROM PRESTAMO
         ORDER BY {pk} DESC
    """
    return query_all(sql)


def crear(data: Dict[str, object]) -> None:
    table = "PRESTAMO"
    pk = first_existing_column(table, ["ID_PRESTAMO", "ID"])
    libro_fk = first_existing_column(table, ["LIBRO_ID_LIBRO", "ID_LIBRO", "LIBRO"])
    usuario_fk = first_existing_column(table, ["USUARIO_ID_USUARIO", "ID_USUARIO", "USUARIO"])
    fp = _fec_prestamo_col()
    fc = _fec_cad_col()

    columns: list[str] = []
    values: list[str] = []
    payload: Dict[str, object] = {}

    if not table_has_identity(table, pk):
        sequence = find_sequence_for(table, pk)
        if sequence:
            columns.append(pk)
            values.append(f"{sequence}.NEXTVAL")
        else:
            columns.append(pk)
            values.append(":_pk")
            payload["_pk"] = next_pk_from_max(table, pk)

    columns.extend([libro_fk, usuario_fk])
    values.extend([":libro", ":usuario"])
    payload["libro"] = data.get("LIBRO_ID") or data.get("ID_LIBRO")
    payload["usuario"] = data.get("USUARIO_ID") or data.get("ID_USUARIO")

    if fp:
        fecha_prestamo = data.get("FECHA_PRESTAMO") or data.get("FECHA_PRESTADO")
        if fecha_prestamo:
            expr = _to_date_expr(table, fp, ":fpr")
            columns.append(fp)
            values.append(expr or ":fpr")
            payload["fpr"] = fecha_prestamo
        elif get_col_datatype(table, fp) == "DATE":
            columns.append(fp)
            values.append("SYSDATE")

    if fc:
        fecha_cad = data.get("FECHA_CADUCIDAD") or data.get("FECHA_DEVOLUCION")
        if fecha_cad:
            expr = _to_date_expr(table, fc, ":fcr")
            columns.append(fc)
            values.append(expr or ":fcr")
            payload["fcr"] = fecha_cad

    if "ESTADO" in data and data["ESTADO"] is not None:
        columns.append("ESTADO")
        values.append(":estado")
        payload["estado"] = data["ESTADO"]

    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)})"
    execute(sql, payload)
