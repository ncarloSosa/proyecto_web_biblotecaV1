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
    t = "PRESTAMO"
    pk = first_existing_column(t, ["ID_PRESTAMO", "ID"])
    lf = first_existing_column(t, ["LIBRO_ID_LIBRO", "ID_LIBRO", "LIBRO"])
    uf = first_existing_column(t, ["USUARIO_ID_USUARIO", "ID_USUARIO", "USUARIO"])

    fp = _fec_prestamo_col()
    fc = _fec_cad_col()

    cols: list[str] = []
    vals: list[str] = []
    payload: Dict[str, object] = {}

    if not table_has_identity(t, pk):
        seq = find_sequence_for(t, pk)
        if seq:
            cols.append(pk)
            vals.append(f"{seq}.NEXTVAL")
        else:
            cols.append(pk)
            vals.append(":pk")
            payload["pk"] = int(next_pk_from_max(t, pk))

    cols.extend([lf, uf])
    vals.extend([":libro", ":usuario"])
    payload["libro"] = data.get("LIBRO_ID") or data.get("ID_LIBRO")
    payload["usuario"] = data.get("USUARIO_ID") or data.get("ID_USUARIO")

    if fp:
        fpr = data.get("FECHA_PRESTAMO") or data.get("FECHA_PRESTADO")
        if fpr:
            expr = (
                "TO_DATE(:fpr,'YYYY-MM-DD')"
                if get_col_datatype(t, fp) == "DATE"
                else ":fpr"
            )
            cols.append(fp)
            vals.append(expr)
            payload["fpr"] = fpr
        elif get_col_datatype(t, fp) == "DATE":
            cols.append(fp)
            vals.append("SYSDATE")

    if fc:
        fcr = data.get("FECHA_CADUCIDAD") or data.get("FECHA_DEVOLUCION")
        if fcr:
            expr = (
                "TO_DATE(:fcr,'YYYY-MM-DD')"
                if get_col_datatype(t, fc) == "DATE"
                else ":fcr"
            )
            cols.append(fc)
            vals.append(expr)
            payload["fcr"] = fcr

    if "ESTADO" in data and data["ESTADO"] is not None:
        cols.append("ESTADO")
        vals.append(":estado")
        payload["estado"] = data["ESTADO"]

    sql = f"INSERT INTO {t} ({', '.join(cols)}) VALUES ({', '.join(vals)})"
    execute(sql, payload)
