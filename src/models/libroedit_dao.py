"""DAO helpers for EDIT_LIB."""

from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, query_all, query_one


def listar() -> List[Dict[str, object]]:
    sql = """
        SELECT ID_EDIT_LIB,
               LIBRO_ID_LIBRO,
               ID_VAREDIT,
               FECHA_EDICION
          FROM EDIT_LIB
         ORDER BY ID_EDIT_LIB DESC
    """
    return query_all(sql)


def obtener(id_edit_lib: int) -> Optional[Dict[str, object]]:
    sql = """
        SELECT ID_EDIT_LIB,
               LIBRO_ID_LIBRO,
               ID_VAREDIT,
               FECHA_EDICION
          FROM EDIT_LIB
         WHERE ID_EDIT_LIB = :ID
    """
    return query_one(sql, {"ID": id_edit_lib})


def crear(data: Dict[str, object]) -> None:
    sql = """
        INSERT INTO EDIT_LIB (
            ID_EDIT_LIB,
            LIBRO_ID_LIBRO,
            ID_VAREDIT,
            FECHA_EDICION
        ) VALUES (
            (SELECT NVL(MAX(ID_EDIT_LIB),0)+1 FROM EDIT_LIB),
            :LIBRO_ID_LIBRO,
            :ID_VAREDIT,
            CASE WHEN :FECHA_EDICION IS NOT NULL THEN TO_DATE(:FECHA_EDICION,'YYYY-MM-DD') ELSE NULL END
        )
    """
    execute(sql, data)


def actualizar(id_edit_lib: int, data: Dict[str, object]) -> None:
    payload = {**data, "ID": id_edit_lib}
    sql = """
        UPDATE EDIT_LIB
           SET LIBRO_ID_LIBRO = :LIBRO_ID_LIBRO,
               ID_VAREDIT = :ID_VAREDIT,
               FECHA_EDICION = CASE
                                 WHEN :FECHA_EDICION IS NOT NULL THEN TO_DATE(:FECHA_EDICION,'YYYY-MM-DD')
                                 ELSE NULL
                               END
         WHERE ID_EDIT_LIB = :ID
    """
    execute(sql, payload)


def eliminar(id_edit_lib: int) -> None:
    execute("DELETE FROM EDIT_LIB WHERE ID_EDIT_LIB = :ID", {"ID": id_edit_lib})
