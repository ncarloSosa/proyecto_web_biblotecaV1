"""DAO helpers for PRESTAMO."""

from __future__ import annotations

from typing import Dict, List, Optional

from .db import (
    any_column_like,
    execute,
    first_existing_column,
    get_col_datatype,
    query_all,
    query_one,
)


def _libro_fk() -> str:
    return first_existing_column("PRESTAMO", ["LIBRO_ID_LIBRO", "ID_LIBRO"])


def _usuario_fk() -> str:
    return first_existing_column("PRESTAMO", ["USUARIO_ID_USUARIO", "ID_USUARIO"])


def _estado_fisico_col() -> Optional[str]:
    candidates = [
        "ESTADO_FISICO",
        "ESTADO_FISIC",
        "EST_FISICO",
        "CONDICION_FISICA",
        "CONDICION",
        "ESTADO_LIBRO",
    ]
    try:
        return first_existing_column("PRESTAMO", candidates)
    except RuntimeError:
        return any_column_like("PRESTAMO", ["FISIC", "CONDIC"])


def _fec_prestamo_col() -> str:
    try:
        return first_existing_column(
            "PRESTAMO", ["FECHA_PRESTAMO", "FECHA_PRESTADO", "FEC_PRESTAMO"]
        )
    except RuntimeError:
        fallback = any_column_like("PRESTAMO", ["FEC_PRE", "FECHA_PRES"])
        return fallback or "FECHA_PRESTAMO"


def _fec_cad_col() -> Optional[str]:
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


def _to_date_expr(table: str, column: str, bind: str) -> str:
    return (
        f"TO_DATE({bind}, 'YYYY-MM-DD')"
        if get_col_datatype(table, column) == "DATE"
        else bind
    )


def listar() -> List[Dict[str, object]]:
    lf = _libro_fk()
    uf = _usuario_fk()
    estfis = _estado_fisico_col()
    fp = _fec_prestamo_col()
    fc = _fec_cad_col()
    select_parts = [
        "ID_PRESTAMO",
        f"{fp} AS FECHA_PRESTAMO",
        f"{fc} AS FECHA_CADUCIDAD" if fc else "NULL AS FECHA_CADUCIDAD",
        "ESTADO",
        f"{lf} AS LIBRO_ID",
        f"{uf} AS USUARIO_ID",
        f"{estfis} AS ESTADO_FISICO" if estfis else "NULL AS ESTADO_FISICO",
    ]
    sql = f"""
        SELECT {', '.join(select_parts)}
          FROM PRESTAMO
         ORDER BY ID_PRESTAMO DESC
    """
    return query_all(sql)


def obtener(id_prestamo: int) -> Optional[Dict[str, object]]:
    lf = _libro_fk()
    uf = _usuario_fk()
    estfis = _estado_fisico_col()
    fp = _fec_prestamo_col()
    fc = _fec_cad_col()
    select_parts = [
        "ID_PRESTAMO",
        f"{fp} AS FECHA_PRESTAMO",
        f"{fc} AS FECHA_CADUCIDAD" if fc else "NULL AS FECHA_CADUCIDAD",
        "ESTADO",
        f"{lf} AS LIBRO_ID",
        f"{uf} AS USUARIO_ID",
        f"{estfis} AS ESTADO_FISICO" if estfis else "NULL AS ESTADO_FISICO",
    ]
    sql = f"""
        SELECT {', '.join(select_parts)}
          FROM PRESTAMO
         WHERE ID_PRESTAMO = :ID
    """
    return query_one(sql, {"ID": id_prestamo})


def crear(data: Dict[str, object]) -> None:
    lf = _libro_fk()
    uf = _usuario_fk()
    estfis = _estado_fisico_col()
    fp = _fec_prestamo_col()
    fc = _fec_cad_col()

    columns = ["ID_PRESTAMO", lf, uf, fp, "ESTADO"]
    values = [
        "(SELECT NVL(MAX(ID_PRESTAMO),0)+1 FROM PRESTAMO)",
        ":LIBRO_ID",
        ":USUARIO_ID",
        _to_date_expr("PRESTAMO", fp, ":FECHA_PRESTAMO"),
        ":ESTADO",
    ]

    payload = {
        "LIBRO_ID": data["LIBRO_ID"],
        "USUARIO_ID": data["USUARIO_ID"],
        "FECHA_PRESTAMO": data["FECHA_PRESTAMO"],
        "ESTADO": data["ESTADO"],
    }

    if fc:
        columns.append(fc)
        values.append(_to_date_expr("PRESTAMO", fc, ":FECHA_CADUCIDAD"))
        payload["FECHA_CADUCIDAD"] = data.get("FECHA_CADUCIDAD")

    if estfis:
        columns.append(estfis)
        values.append(":ESTADO_FISICO")
        payload["ESTADO_FISICO"] = data.get("ESTADO_FISICO")

    sql = f"""
        INSERT INTO PRESTAMO ({', '.join(columns)})
        VALUES ({', '.join(values)})
    """
    execute(sql, payload)


def actualizar(id_prestamo: int, data: Dict[str, object]) -> None:
    lf = _libro_fk()
    uf = _usuario_fk()
    estfis = _estado_fisico_col()
    fp = _fec_prestamo_col()
    fc = _fec_cad_col()

    set_parts = [
        f"{lf} = :LIBRO_ID",
        f"{uf} = :USUARIO_ID",
        f"{fp} = {_to_date_expr('PRESTAMO', fp, ':FECHA_PRESTAMO')}",
        "ESTADO = :ESTADO",
    ]

    payload: Dict[str, object] = {
        "ID": id_prestamo,
        "LIBRO_ID": data["LIBRO_ID"],
        "USUARIO_ID": data["USUARIO_ID"],
        "FECHA_PRESTAMO": data["FECHA_PRESTAMO"],
        "ESTADO": data["ESTADO"],
    }

    if fc:
        set_parts.append(f"{fc} = {_to_date_expr('PRESTAMO', fc, ':FECHA_CADUCIDAD')}")
        payload["FECHA_CADUCIDAD"] = data.get("FECHA_CADUCIDAD")

    if estfis:
        set_parts.append(f"{estfis} = :ESTADO_FISICO")
        payload["ESTADO_FISICO"] = data.get("ESTADO_FISICO")

    sql = f"""
        UPDATE PRESTAMO
           SET {', '.join(set_parts)}
         WHERE ID_PRESTAMO = :ID
    """
    execute(sql, payload)


def eliminar(id_prestamo: int) -> None:
    execute("DELETE FROM PRESTAMO WHERE ID_PRESTAMO = :ID", {"ID": id_prestamo})
