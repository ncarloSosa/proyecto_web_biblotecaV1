"""DAO helpers for MIEMBRO."""

from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, next_id, query_all, query_one


def listar() -> List[Dict[str, object]]:
    sql = """
        SELECT ID_MIEMBRO, ID_USUARIO, ID_GRUPO
          FROM MIEMBRO
         ORDER BY ID_MIEMBRO DESC
    """
    return query_all(sql)


def obtener(id_miembro: int) -> Optional[Dict[str, object]]:
    sql = """
        SELECT ID_MIEMBRO, ID_USUARIO, ID_GRUPO
          FROM MIEMBRO
         WHERE ID_MIEMBRO = :ID
    """
    return query_one(sql, {"ID": id_miembro})


def crear(data: Dict[str, object]) -> int:
    new_id = next_id("MIEMBRO", "ID_MIEMBRO")
    payload = {**data, "ID_MIEMBRO": new_id}
    sql = """
        INSERT INTO MIEMBRO (ID_MIEMBRO, ID_USUARIO, ID_GRUPO)
        VALUES (:ID_MIEMBRO, :ID_USUARIO, :ID_GRUPO)
    """
    execute(sql, payload)
    return new_id


def actualizar(id_miembro: int, data: Dict[str, object]) -> None:
    payload = {**data, "ID": id_miembro}
    sql = """
        UPDATE MIEMBRO
           SET ID_USUARIO = :ID_USUARIO,
               ID_GRUPO = :ID_GRUPO
         WHERE ID_MIEMBRO = :ID
    """
    execute(sql, payload)


def eliminar(id_miembro: int) -> None:
    execute("DELETE FROM MIEMBRO WHERE ID_MIEMBRO = :ID", {"ID": id_miembro})
