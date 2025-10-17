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
    query_one,
    table_has_identity,
    next_pk_from_max,
)


def _pk() -> str:
    return first_existing_column("PRESTAMO", ["ID_PRESTAMO", "ID"])


def _libro_fk() -> str:
    return first_existing_column("PRESTAMO", ["LIBRO_ID_LIBRO", "ID_LIBRO", "LIBRO"])


def _usuario_fk() -> str:
    candidates = [
        "USUARIO_ID_USUARIO",
        "ID_USUARIO",
        "USUARIO",
        "ID_MIEMBRO",
        "MIEMBRO",
        "ID_CLIENTE",
        "CLIENTE",
    ]
    return first_existing_column("PRESTAMO", candidates)


def _fecha_prestamo_col() -> str | None:
    for candidate in ["FECHA_PRESTAMO", "FECHA_PRESTADO", "FEC_PRESTAMO"]:
        try:
            return first_existing_column("PRESTAMO", [candidate])
        except RuntimeError:
            continue
    return any_column_like("PRESTAMO", ["FEC_PRE", "FECHA_PRES"])


def _fecha_cad_col() -> str | None:
    for candidate in [
        "FECHA_CADUCIDAD",
        "FECHA_VENCIMIENTO",
        "FECHA_DEVOLUCION",
        "FEC_CADUCIDAD",
        "FEC_VENCIMIENTO",
        "FEC_DEVOLUCION",
    ]:
        try:
            return first_existing_column("PRESTAMO", [candidate])
        except RuntimeError:
            continue
    return any_column_like("PRESTAMO", ["CADUC", "VENC", "DEVOL"])


def _estado_col() -> str | None:
    return any_column_like("PRESTAMO", ["ESTADO", "STATUS", "ESTADO_FISICO"])


def _format_date_select(table: str, column: str | None) -> str:
    if not column:
        return "NULL"
    if get_col_datatype(table, column) == "DATE":
        return f"TO_CHAR({column}, 'YYYY-MM-DD')"
    return column


def listar() -> List[Dict[str, object]]:
    table = "PRESTAMO"
    pk = _pk()
    libro_fk = _libro_fk()
    usuario_fk = _usuario_fk()
    fp = _fecha_prestamo_col()
    fc = _fecha_cad_col()
    estado_col = _estado_col() or "NULL"

    sql = f"""
        SELECT {pk} AS ID_PRESTAMO,
               {libro_fk} AS LIBRO_ID,
               {usuario_fk} AS USUARIO_ID,
               {_format_date_select(table, fp)} AS FECHA_PRESTAMO,
               {_format_date_select(table, fc)} AS FECHA_CADUCIDAD,
               {estado_col} AS ESTADO
          FROM {table}
         ORDER BY ID_PRESTAMO DESC
    """
    return query_all(sql)


def obtener(id_prestamo: int) -> Dict[str, object] | None:
    table = "PRESTAMO"
    pk = _pk()
    libro_fk = _libro_fk()
    usuario_fk = _usuario_fk()
    fp = _fecha_prestamo_col()
    fc = _fecha_cad_col()
    estado_col = _estado_col() or "NULL"

    sql = f"""
        SELECT {pk} AS ID_PRESTAMO,
               {libro_fk} AS LIBRO_ID,
               {usuario_fk} AS USUARIO_ID,
               {_format_date_select(table, fp)} AS FECHA_PRESTAMO,
               {_format_date_select(table, fc)} AS FECHA_CADUCIDAD,
               {estado_col} AS ESTADO
          FROM {table}
         WHERE {pk} = :id
    """
    return query_one(sql, {"id": int(id_prestamo)})


def crear(data: Dict[str, object]) -> None:
    """Insert a loan row using schema-aware metadata for binds."""

    table = "PRESTAMO"
    pk = _pk()
    libro_fk = _libro_fk()
    usuario_fk = _usuario_fk()
    fp = _fecha_prestamo_col()
    fc = _fecha_cad_col()
    estado_col = _estado_col()

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
            values.append(":pk")
            payload["pk"] = int(next_pk_from_max(table, pk))

    columns.extend([libro_fk, usuario_fk])
    values.extend([":libro", ":usuario"])
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

    if fp:
        fecha_prestamo = data.get("FECHA_PRESTAMO") or data.get("FECHA_PRESTADO")
        if fecha_prestamo:
            expr = (
                "TO_DATE(:fpr,'YYYY-MM-DD')"
                if get_col_datatype(table, fp) == "DATE"
                else ":fpr"
            )
            columns.append(fp)
            values.append(expr)
            payload["fpr"] = fecha_prestamo
        elif get_col_datatype(table, fp) == "DATE":
            columns.append(fp)
            values.append("SYSDATE")

    if fc:
        fecha_cad = (
            data.get("FECHA_CADUCIDAD")
            or data.get("FECHA_VENCIMIENTO")
            or data.get("FECHA_DEVOLUCION")
        )
        if fecha_cad:
            expr = (
                "TO_DATE(:fcr,'YYYY-MM-DD')"
                if get_col_datatype(table, fc) == "DATE"
                else ":fcr"
            )
            columns.append(fc)
            values.append(expr)
            payload["fcr"] = fecha_cad

    if estado_col and data.get("ESTADO") is not None:
        columns.append(estado_col)
        values.append(":estado")
        payload["estado"] = data["ESTADO"]

    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)})"
    execute(sql, payload)
