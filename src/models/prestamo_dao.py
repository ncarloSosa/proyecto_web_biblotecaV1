"""DAO helpers for PRESTAMO."""

from __future__ import annotations

from typing import Dict, List, Optional

from .db import any_column_like, execute, first_existing_column, query_all, query_one


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


def listar() -> List[Dict[str, object]]:
    lf = _libro_fk()
    uf = _usuario_fk()
    estfis = _estado_fisico_col()
    est_select = f", {estfis} AS ESTADO_FISICO" if estfis else ", NULL AS ESTADO_FISICO"
    sql = f"""
        SELECT ID_PRESTAMO,
               FECHA_PRESTADO,
               FECHA_CADUCIDAD,
               ESTADO,
               {lf} AS LIBRO_ID,
               {uf} AS USUARIO_ID
               {est_select}
          FROM PRESTAMO
         ORDER BY ID_PRESTAMO DESC
    """
    return query_all(sql)


def obtener(id_prestamo: int) -> Optional[Dict[str, object]]:
    lf = _libro_fk()
    uf = _usuario_fk()
    estfis = _estado_fisico_col()
    est_select = f", {estfis} AS ESTADO_FISICO" if estfis else ", NULL AS ESTADO_FISICO"
    sql = f"""
        SELECT ID_PRESTAMO,
               FECHA_PRESTADO,
               FECHA_CADUCIDAD,
               ESTADO,
               {lf} AS LIBRO_ID,
               {uf} AS USUARIO_ID
               {est_select}
          FROM PRESTAMO
         WHERE ID_PRESTAMO = :ID
    """
    return query_one(sql, {"ID": id_prestamo})


def crear(data: Dict[str, object]) -> None:
    lf = _libro_fk()
    uf = _usuario_fk()
    estfis = _estado_fisico_col()
    columns = [
        "ID_PRESTAMO",
        "FECHA_PRESTADO",
        "FECHA_CADUCIDAD",
        "ESTADO",
    ]
    values = [
        "(SELECT NVL(MAX(ID_PRESTAMO),0)+1 FROM PRESTAMO)",
        "TO_DATE(:FECHA_PRESTADO,'YYYY-MM-DD')",
        "TO_DATE(:FECHA_CADUCIDAD,'YYYY-MM-DD')",
        ":ESTADO",
    ]
    payload = dict(data)
    if estfis:
        columns.append(estfis)
        values.append(":ESTADO_FISICO")
    else:
        payload.pop("ESTADO_FISICO", None)
    columns.extend([lf, uf])
    values.extend([":LIBRO_ID", ":USUARIO_ID"])
    sql = f"""
        INSERT INTO PRESTAMO ({', '.join(columns)})
        VALUES ({', '.join(values)})
    """
    execute(sql, payload)


def actualizar(id_prestamo: int, data: Dict[str, object]) -> None:
    lf = _libro_fk()
    uf = _usuario_fk()
    estfis = _estado_fisico_col()
    payload = {**data, "ID": id_prestamo}
    set_parts = [
        "FECHA_PRESTADO = TO_DATE(:FECHA_PRESTADO,'YYYY-MM-DD')",
        "FECHA_CADUCIDAD = TO_DATE(:FECHA_CADUCIDAD,'YYYY-MM-DD')",
        "ESTADO = :ESTADO",
    ]
    if estfis:
        set_parts.append(f"{estfis} = :ESTADO_FISICO")
    else:
        payload.pop("ESTADO_FISICO", None)
    set_parts.append(f"{lf} = :LIBRO_ID")
    set_parts.append(f"{uf} = :USUARIO_ID")
    sql = f"""
        UPDATE PRESTAMO
           SET {', '.join(set_parts)}
         WHERE ID_PRESTAMO = :ID
    """
    execute(sql, payload)


def eliminar(id_prestamo: int) -> None:
    execute("DELETE FROM PRESTAMO WHERE ID_PRESTAMO = :ID", {"ID": id_prestamo})
