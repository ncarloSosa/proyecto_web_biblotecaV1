"""DAO for EDITORIAL."""
from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, first_existing_column, next_id, query_all, query_one


def _pk() -> str:
    return first_existing_column("EDITORIAL", ["ID_EDITORIAL", "ID_VAREDIT", "NUM_EDITORIAL"])


def listar() -> List[Dict[str, object]]:
    pk = _pk()
    sql = f"""
        SELECT {pk} AS EDITORIAL_ID, NOMBRE, PAIS, ANO_EDICION, NUM_EDITORIAL
          FROM EDITORIAL
         ORDER BY {pk} DESC
    """
    return query_all(sql)


def obtener(id_editorial: int) -> Optional[Dict[str, object]]:
    pk = _pk()
    sql = f"""
        SELECT {pk} AS EDITORIAL_ID, NOMBRE, PAIS, ANO_EDICION, NUM_EDITORIAL
          FROM EDITORIAL
         WHERE {pk} = :ID
    """
    return query_one(sql, {"ID": id_editorial})


def crear(data: Dict[str, object]) -> int:
    pk = _pk()
    new_id = next_id("EDITORIAL", pk)
    payload = {**data, pk: new_id}
    sql = f"""
        INSERT INTO EDITORIAL
          ({pk}, NOMBRE, PAIS, ANO_EDICION, NUM_EDITORIAL)
        VALUES
          (:{pk}, :NOMBRE, :PAIS, :ANO_EDICION, :NUM_EDITORIAL)
    """
    execute(sql, payload)
    return new_id


def actualizar(id_editorial: int, data: Dict[str, object]) -> None:
    pk = _pk()
    payload = {**data, "ID": id_editorial}
    sql = f"""
        UPDATE EDITORIAL
           SET NOMBRE = :NOMBRE,
               PAIS = :PAIS,
               ANO_EDICION = :ANO_EDICION,
               NUM_EDITORIAL = :NUM_EDITORIAL
         WHERE {pk} = :ID
    """
    execute(sql, payload)


def eliminar(id_editorial: int) -> None:
    pk = _pk()
    execute(f"DELETE FROM EDITORIAL WHERE {pk} = :ID", {"ID": id_editorial})
