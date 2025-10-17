"""DAO helpers for PRESTAMO with dynamic schema detection."""

from __future__ import annotations

from typing import Dict, List

from .db import (
    any_column_like,
    execute,
    first_existing_column,
    get_col_datatype,
    query_all,
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
    libro_fk = first_existing_column("PRESTAMO", ["LIBRO_ID_LIBRO", "ID_LIBRO", "LIBRO"])
    usuario_fk = first_existing_column(
        "PRESTAMO", ["USUARIO_ID_USUARIO", "ID_USUARIO", "USUARIO"]
    )
    fp = _fec_prestamo_col()
    fc = _fec_cad_col()

    columns: list[str] = [libro_fk, usuario_fk]
    values: list[str] = [":libro", ":usuario"]
    payload: Dict[str, object] = {
        "libro": data.get("LIBRO_ID") or data.get("ID_LIBRO"),
        "usuario": data.get("USUARIO_ID") or data.get("ID_USUARIO"),
    }

    # Fecha de préstamo (opcional, por defecto SYSDATE si la columna existe y es DATE)
    if fp:
        fecha_prestamo = data.get("FECHA_PRESTAMO") or data.get("FECHA_PRESTADO")
        expr = _to_date_expr("PRESTAMO", fp, ":f_prestamo")
        if fecha_prestamo:
            columns.append(fp)
            values.append(expr or ":f_prestamo")
            payload["f_prestamo"] = fecha_prestamo
        elif get_col_datatype("PRESTAMO", fp) == "DATE":
            columns.append(fp)
            values.append("SYSDATE")

    # Fecha de caducidad opcional
    if fc:
        fecha_cad = data.get("FECHA_CADUCIDAD") or data.get("FECHA_DEVOLUCION")
        if fecha_cad:
            expr = _to_date_expr("PRESTAMO", fc, ":f_cad")
            columns.append(fc)
            values.append(expr or ":f_cad")
            payload["f_cad"] = fecha_cad

    if "ESTADO" in data and data["ESTADO"] is not None:
        columns.append("ESTADO")
        values.append(":estado")
        payload["estado"] = data["ESTADO"]

    sql = f"""
        INSERT INTO PRESTAMO ({', '.join(columns)})
        VALUES ({', '.join(values)})
    """
    execute(sql, payload)
