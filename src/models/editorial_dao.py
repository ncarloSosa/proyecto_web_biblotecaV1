"""DAO for EDITORIAL."""
from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, query_all, query_one


def listar() -> List[Dict[str, object]]:
    sql = """
        SELECT ID_EDITORIAL, NOMBRE, PAIS, ANO_EDICION, NUM_EDITORIAL
        FROM EDITORIAL
        ORDER BY ID_EDITORIAL DESC
    """
    return query_all(sql)


def obtener(id_editorial: int) -> Optional[Dict[str, object]]:
    sql = """
        SELECT ID_EDITORIAL, NOMBRE, PAIS, ANO_EDICION, NUM_EDITORIAL
        FROM EDITORIAL
        WHERE ID_EDITORIAL = :id
    """
    return query_one(sql, {"id": id_editorial})


def crear(data: Dict[str, object]) -> None:
    sql = """
        INSERT INTO EDITORIAL
          (ID_EDITORIAL, NOMBRE, PAIS, ANO_EDICION, NUM_EDITORIAL)
        VALUES
          (EDITORIAL_SEQ.NEXTVAL, :NOMBRE, :PAIS, :ANO_EDICION, :NUM_EDITORIAL)
    """
    execute(sql, data)


def actualizar(id_editorial: int, data: Dict[str, object]) -> None:
    payload = {**data, "ID": id_editorial}
    sql = """
        UPDATE EDITORIAL
           SET NOMBRE = :NOMBRE,
               PAIS = :PAIS,
               ANO_EDICION = :ANO_EDICION,
               NUM_EDITORIAL = :NUM_EDITORIAL
         WHERE ID_EDITORIAL = :ID
    """
    execute(sql, payload)


def eliminar(id_editorial: int) -> None:
    execute("DELETE FROM EDITORIAL WHERE ID_EDITORIAL = :id", {"id": id_editorial})
