"""DAO helpers for PRESTAMO."""

from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, first_existing_column, query_all, query_one


def _libro_fk() -> str:
    return first_existing_column("PRESTAMO", ["LIBRO_ID_LIBRO", "ID_LIBRO"])


def _usuario_fk() -> str:
    return first_existing_column("PRESTAMO", ["USUARIO_ID_USUARIO", "ID_USUARIO"])


def listar() -> List[Dict[str, object]]:
    lf = _libro_fk()
    uf = _usuario_fk()
    sql = f"""
        SELECT ID_PRESTAMO,
               FECHA_PRESTADO,
               FECHA_CADUCIDAD,
               ESTADO,
               ESTADO_FISICO,
               {lf} AS LIBRO_ID,
               {uf} AS USUARIO_ID
          FROM PRESTAMO
         ORDER BY ID_PRESTAMO DESC
    """
    return query_all(sql)


def obtener(id_prestamo: int) -> Optional[Dict[str, object]]:
    lf = _libro_fk()
    uf = _usuario_fk()
    sql = f"""
        SELECT ID_PRESTAMO,
               FECHA_PRESTADO,
               FECHA_CADUCIDAD,
               ESTADO,
               ESTADO_FISICO,
               {lf} AS LIBRO_ID,
               {uf} AS USUARIO_ID
          FROM PRESTAMO
         WHERE ID_PRESTAMO = :ID
    """
    return query_one(sql, {"ID": id_prestamo})


def crear(data: Dict[str, object]) -> None:
    lf = _libro_fk()
    uf = _usuario_fk()
    sql = f"""
        INSERT INTO PRESTAMO (
            ID_PRESTAMO,
            FECHA_PRESTADO,
            FECHA_CADUCIDAD,
            ESTADO,
            ESTADO_FISICO,
            {lf},
            {uf}
        ) VALUES (
            (SELECT NVL(MAX(ID_PRESTAMO),0)+1 FROM PRESTAMO),
            TO_DATE(:FECHA_PRESTADO,'YYYY-MM-DD'),
            TO_DATE(:FECHA_CADUCIDAD,'YYYY-MM-DD'),
            :ESTADO,
            :ESTADO_FISICO,
            :LIBRO_ID,
            :USUARIO_ID
        )
    """
    execute(sql, data)


def actualizar(id_prestamo: int, data: Dict[str, object]) -> None:
    lf = _libro_fk()
    uf = _usuario_fk()
    payload = {**data, "ID": id_prestamo}
    sql = f"""
        UPDATE PRESTAMO
           SET FECHA_PRESTADO = TO_DATE(:FECHA_PRESTADO,'YYYY-MM-DD'),
               FECHA_CADUCIDAD = TO_DATE(:FECHA_CADUCIDAD,'YYYY-MM-DD'),
               ESTADO = :ESTADO,
               ESTADO_FISICO = :ESTADO_FISICO,
               {lf} = :LIBRO_ID,
               {uf} = :USUARIO_ID
         WHERE ID_PRESTAMO = :ID
    """
    execute(sql, payload)


def eliminar(id_prestamo: int) -> None:
    execute("DELETE FROM PRESTAMO WHERE ID_PRESTAMO = :ID", {"ID": id_prestamo})
