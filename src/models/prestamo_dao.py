"""DAO helpers for PRESTAMO."""

from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, query_all, query_one


def _column_exists(name: str) -> bool:
    sql = """
        SELECT 1
          FROM USER_TAB_COLUMNS
         WHERE TABLE_NAME = 'PRESTAMO'
           AND COLUMN_NAME = :NAME
    """
    return query_one(sql, {"NAME": name.upper()}) is not None


def _resolve_column(preferred: str, *alternatives: str) -> str:
    if _column_exists(preferred):
        return preferred
    for option in alternatives:
        if _column_exists(option):
            return option
    return preferred


_USER_COL = _resolve_column("ID_USUARIO", "USUARIO_ID_USUARIO")
_BOOK_COL = _resolve_column("ID_LIBRO", "LIBRO_ID_LIBRO")
_CADUC_COL = _resolve_column("FECHA_CADUCIDAD", "FECH_CADUC")
_ESTADO_FISICO_COL = _resolve_column("ESTADO_FISICO", "ESTADO_FISIC")


def listar() -> List[Dict[str, object]]:
    sql = f"""
        SELECT ID_PRESTAMO,
               FECHA_PRESTADO,
               {_CADUC_COL} AS FECHA_CADUCIDAD,
               ESTADO,
               {_ESTADO_FISICO_COL} AS ESTADO_FISICO,
               {_BOOK_COL} AS ID_LIBRO,
               {_USER_COL} AS ID_USUARIO
          FROM PRESTAMO
         ORDER BY ID_PRESTAMO DESC
    """
    return query_all(sql)


def obtener(id_prestamo: int) -> Optional[Dict[str, object]]:
    sql = f"""
        SELECT ID_PRESTAMO,
               FECHA_PRESTADO,
               {_CADUC_COL} AS FECHA_CADUCIDAD,
               ESTADO,
               {_ESTADO_FISICO_COL} AS ESTADO_FISICO,
               {_BOOK_COL} AS ID_LIBRO,
               {_USER_COL} AS ID_USUARIO
          FROM PRESTAMO
         WHERE ID_PRESTAMO = :ID
    """
    return query_one(sql, {"ID": id_prestamo})


def crear(data: Dict[str, object]) -> None:
    sql = f"""
        INSERT INTO PRESTAMO (
            ID_PRESTAMO,
            FECHA_PRESTADO,
            {_CADUC_COL},
            ESTADO,
            {_ESTADO_FISICO_COL},
            {_BOOK_COL},
            {_USER_COL}
        ) VALUES (
            PRESTAMO_SEQ.NEXTVAL,
            TO_DATE(:FECHA_PRESTADO,'YYYY-MM-DD'),
            TO_DATE(:FECHA_CADUCIDAD,'YYYY-MM-DD'),
            :ESTADO,
            :ESTADO_FISICO,
            :ID_LIBRO,
            :ID_USUARIO
        )
    """
    execute(sql, data)


def actualizar(id_prestamo: int, data: Dict[str, object]) -> None:
    payload = {**data, "ID": id_prestamo}
    sql = f"""
        UPDATE PRESTAMO
           SET FECHA_PRESTADO = TO_DATE(:FECHA_PRESTADO,'YYYY-MM-DD'),
               {_CADUC_COL} = TO_DATE(:FECHA_CADUCIDAD,'YYYY-MM-DD'),
               ESTADO = :ESTADO,
               {_ESTADO_FISICO_COL} = :ESTADO_FISICO,
               {_BOOK_COL} = :ID_LIBRO,
               {_USER_COL} = :ID_USUARIO
         WHERE ID_PRESTAMO = :ID
    """
    execute(sql, payload)


def eliminar(id_prestamo: int) -> None:
    execute("DELETE FROM PRESTAMO WHERE ID_PRESTAMO = :ID", {"ID": id_prestamo})
