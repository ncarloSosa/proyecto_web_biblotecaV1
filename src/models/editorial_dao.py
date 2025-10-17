"""DAO for EDITORIAL with schema-aware helpers."""
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

from .db import (
    column_exists,
    execute,
    first_existing_column,
    get_col_datatype,
    next_id,
    query_all,
    query_one,
)


def _pk() -> str:
    return first_existing_column("EDITORIAL", ["ID_EDITORIAL", "ID_VAREDIT", "NUM_EDITORIAL"])


def _year_column() -> str:
    return first_existing_column(
        "EDITORIAL",
        ["ANO_EDICION", "ANIO_EDICION", "FECHA_EDICION", "FECHA"],
    )


def _num_column() -> Optional[str]:
    for candidate in ("NUM_EDITORIAL", "NUMERO_EDITORIAL"):
        if column_exists("EDITORIAL", candidate):
            return candidate
    return None


def _prepare_year(value: object) -> tuple[str, Dict[str, object]]:
    year_col = _year_column()
    datatype = get_col_datatype("EDITORIAL", year_col)
    text = "" if value is None else str(value).strip()
    if not text:
        raise ValueError("El año de edición es obligatorio.")

    if datatype == "DATE":
        if len(text) == 4 and text.isdigit():
            normalized = f"{text}-01-01"
        else:
            try:
                datetime.strptime(text[:10], "%Y-%m-%d")
            except ValueError as exc:
                raise ValueError("El año de edición debe tener formato YYYY o YYYY-MM-DD.") from exc
            normalized = text[:10]
        return year_col, "TO_DATE(:ANO_EDICION_VAL,'YYYY-MM-DD')", {"ANO_EDICION_VAL": normalized}

    if datatype == "NUMBER":
        try:
            numeric = int(float(text))
        except ValueError as exc:
            raise ValueError("El año de edición debe ser numérico.") from exc
        return year_col, ":ANO_EDICION_VAL", {"ANO_EDICION_VAL": numeric}

    # Default to storing as text
    return year_col, ":ANO_EDICION_VAL", {"ANO_EDICION_VAL": text}


def listar() -> List[Dict[str, object]]:
    pk = _pk()
    year_col = _year_column()
    num_col = _num_column()
    num_select = f", {num_col} AS NUM_EDITORIAL" if num_col else ""
    sql = f"""
        SELECT {pk} AS EDITORIAL_ID,
               NOMBRE,
               PAIS,
               {year_col} AS ANO_EDICION{num_select}
          FROM EDITORIAL
         ORDER BY {pk} DESC
    """
    return query_all(sql)


def obtener(id_editorial: int) -> Optional[Dict[str, object]]:
    pk = _pk()
    year_col = _year_column()
    num_col = _num_column()
    num_select = f", {num_col} AS NUM_EDITORIAL" if num_col else ""
    sql = f"""
        SELECT {pk} AS EDITORIAL_ID,
               NOMBRE,
               PAIS,
               {year_col} AS ANO_EDICION{num_select}
          FROM EDITORIAL
         WHERE {pk} = :ID
    """
    return query_one(sql, {"ID": id_editorial})


def crear(data: Dict[str, object]) -> int:
    pk = _pk()
    year_col, year_expr, year_payload = _prepare_year(data.get("ANO_EDICION"))
    num_col = _num_column()
    new_id = next_id("EDITORIAL", pk)

    columns = [pk, "NOMBRE", "PAIS", year_col]
    values = [f":{pk}", ":NOMBRE", ":PAIS", year_expr]
    payload: Dict[str, object] = {
        pk: new_id,
        "NOMBRE": data["NOMBRE"],
        "PAIS": data.get("PAIS"),
    }
    payload.update(year_payload)

    if num_col:
        columns.append(num_col)
        values.append(":NUM_EDITORIAL")
        payload["NUM_EDITORIAL"] = data.get("NUM_EDITORIAL")

    sql = f"""
        INSERT INTO EDITORIAL ({', '.join(columns)})
        VALUES ({', '.join(values)})
    """
    execute(sql, payload)
    return new_id


def actualizar(id_editorial: int, data: Dict[str, object]) -> None:
    pk = _pk()
    year_col, year_expr, year_payload = _prepare_year(data.get("ANO_EDICION"))
    num_col = _num_column()

    set_parts = [
        "NOMBRE = :NOMBRE",
        "PAIS = :PAIS",
        f"{year_col} = {year_expr}",
    ]
    payload: Dict[str, object] = {
        "ID": id_editorial,
        "NOMBRE": data["NOMBRE"],
        "PAIS": data.get("PAIS"),
    }
    payload.update(year_payload)

    if num_col:
        set_parts.append(f"{num_col} = :NUM_EDITORIAL")
        payload["NUM_EDITORIAL"] = data.get("NUM_EDITORIAL")

    sql = f"""
        UPDATE EDITORIAL
           SET {', '.join(set_parts)}
         WHERE {pk} = :ID
    """
    execute(sql, payload)


def eliminar(id_editorial: int) -> None:
    pk = _pk()
    execute(f"DELETE FROM EDITORIAL WHERE {pk} = :ID", {"ID": id_editorial})
