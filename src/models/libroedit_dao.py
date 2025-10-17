"""DAO helpers for LIBRO_EDIT."""

from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, next_id, query_all, query_one


def listar() -> List[Dict[str, object]]:
    sql = """
        SELECT ID_LIBRO_EDIT, ID_EDITORIAL, ID_LIBRO, ID_AUTOR
          FROM LIBRO_EDIT
         ORDER BY ID_LIBRO_EDIT DESC
    """
    return query_all(sql)


def obtener(id_libro_edit: int) -> Optional[Dict[str, object]]:
    sql = """
        SELECT ID_LIBRO_EDIT, ID_EDITORIAL, ID_LIBRO, ID_AUTOR
          FROM LIBRO_EDIT
         WHERE ID_LIBRO_EDIT = :ID
    """
    return query_one(sql, {"ID": id_libro_edit})


def crear(data: Dict[str, object]) -> int:
    new_id = next_id("LIBRO_EDIT", "ID_LIBRO_EDIT")
    payload = {**data, "ID_LIBRO_EDIT": new_id}
    sql = """
        INSERT INTO LIBRO_EDIT (ID_LIBRO_EDIT, ID_EDITORIAL, ID_LIBRO, ID_AUTOR)
        VALUES (:ID_LIBRO_EDIT, :ID_EDITORIAL, :ID_LIBRO, :ID_AUTOR)
    """
    execute(sql, payload)
    return new_id


def actualizar(id_libro_edit: int, data: Dict[str, object]) -> None:
    payload = {**data, "ID": id_libro_edit}
    sql = """
        UPDATE LIBRO_EDIT
           SET ID_EDITORIAL = :ID_EDITORIAL,
               ID_LIBRO = :ID_LIBRO,
               ID_AUTOR = :ID_AUTOR
         WHERE ID_LIBRO_EDIT = :ID
    """
    execute(sql, payload)


def eliminar(id_libro_edit: int) -> None:
    execute("DELETE FROM LIBRO_EDIT WHERE ID_LIBRO_EDIT = :ID", {"ID": id_libro_edit})
