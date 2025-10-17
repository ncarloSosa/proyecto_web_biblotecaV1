"""DAO helpers for UBICACION."""

from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, next_id, query_all, query_one


def listar() -> List[Dict[str, object]]:
    sql = """
        SELECT ID_UBICACION, ESTANTERIA, DESCRIPCION
          FROM UBICACION
         ORDER BY ID_UBICACION DESC
    """
    return query_all(sql)


def obtener(id_ubicacion: int) -> Optional[Dict[str, object]]:
    sql = """
        SELECT ID_UBICACION, ESTANTERIA, DESCRIPCION
          FROM UBICACION
         WHERE ID_UBICACION = :ID
    """
    return query_one(sql, {"ID": id_ubicacion})


def crear(data: Dict[str, object]) -> int:
    new_id = next_id("UBICACION", "ID_UBICACION")
    payload = {**data, "ID_UBICACION": new_id}
    sql = """
        INSERT INTO UBICACION (ID_UBICACION, ESTANTERIA, DESCRIPCION)
        VALUES (:ID_UBICACION, :ESTANTERIA, :DESCRIPCION)
    """
    execute(sql, payload)
    return new_id


def actualizar(id_ubicacion: int, data: Dict[str, object]) -> None:
    payload = {**data, "ID": id_ubicacion}
    sql = """
        UPDATE UBICACION
           SET ESTANTERIA = :ESTANTERIA,
               DESCRIPCION = :DESCRIPCION
         WHERE ID_UBICACION = :ID
    """
    execute(sql, payload)


def eliminar(id_ubicacion: int) -> None:
    execute("DELETE FROM UBICACION WHERE ID_UBICACION = :ID", {"ID": id_ubicacion})
