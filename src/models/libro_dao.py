"""DAO helpers for LIBRO."""

from __future__ import annotations

from typing import Dict, List, Optional

from .db import column_exists, execute, query_all, query_one


def _pub_col() -> str:
    return "FECHA_PUBLICACION" if column_exists("LIBRO", "FECHA_PUBLICACION") else "ANO_PUBL"


def listar() -> List[Dict[str, object]]:
    col = _pub_col()
    sql = f"""
    SELECT ID_LIBRO,
           TITULO,
           ISBN,
           {col} AS FECHA_PUBLICACION,
           CLASIFICACION,
           ESTADO_FISICO,
           FECHA_REGISTRO,
           ID_VAREDIT,
           ID_GENERO,
           ID_IDIOMA
      FROM LIBRO
     ORDER BY ID_LIBRO DESC
    """
    return query_all(sql)


def obtener(id_libro: int) -> Optional[Dict[str, object]]:
    col = _pub_col()
    sql = f"""
    SELECT ID_LIBRO,
           TITULO,
           SUBTITULO,
           ISBN,
           {col} AS FECHA_PUBLICACION,
           NUM_COPIAS,
           NUM_PAGINAS,
           DESCRIPCION,
           CLASIFICACION,
           PERTENECE_GRUPO,
           ESTADO_FISICO,
           ID_VAREDIT,
           ID_GENERO,
           ID_IDIOMA
      FROM LIBRO
     WHERE ID_LIBRO = :ID
    """
    return query_one(sql, {"ID": id_libro})


def crear(data: Dict[str, object]) -> None:
    col = _pub_col()
    sql = f"""
    INSERT INTO LIBRO
      (ID_LIBRO, TITULO, SUBTITULO, ISBN, {col}, NUM_COPIAS, NUM_PAGINAS,
       FECHA_REGISTRO, DESCRIPCION, CLASIFICACION, PERTENECE_GRUPO, ESTADO_FISICO,
       ID_VAREDIT, ID_GENERO, ID_IDIOMA)
    VALUES
      ((SELECT NVL(MAX(ID_LIBRO),0)+1 FROM LIBRO),
       :TITULO, :SUBTITULO, :ISBN, TO_DATE(:FECHA_PUBLICACION,'YYYY-MM-DD'),
       :NUM_COPIAS, :NUM_PAGINAS, SYSDATE, :DESCRIPCION, :CLASIFICACION,
       :PERTENECE_GRUPO, :ESTADO_FISICO, :ID_VAREDIT, :ID_GENERO, :ID_IDIOMA)
    """
    execute(sql, data)


def actualizar(id_libro: int, data: Dict[str, object]) -> None:
    col = _pub_col()
    sql = f"""
    UPDATE LIBRO
       SET TITULO = :TITULO,
           SUBTITULO = :SUBTITULO,
           ISBN = :ISBN,
           {col} = TO_DATE(:FECHA_PUBLICACION,'YYYY-MM-DD'),
           NUM_COPIAS = :NUM_COPIAS,
           NUM_PAGINAS = :NUM_PAGINAS,
           DESCRIPCION = :DESCRIPCION,
           CLASIFICACION = :CLASIFICACION,
           PERTENECE_GRUPO = :PERTENECE_GRUPO,
           ESTADO_FISICO = :ESTADO_FISICO,
           ID_VAREDIT = :ID_VAREDIT,
           ID_GENERO = :ID_GENERO,
           ID_IDIOMA = :ID_IDIOMA
     WHERE ID_LIBRO = :ID
    """
    execute(sql, dict(data, ID=id_libro))


def eliminar(id_libro: int) -> None:
    execute("DELETE FROM LIBRO WHERE ID_LIBRO = :ID", {"ID": id_libro})


def reporte() -> List[Dict[str, object]]:
    col = _pub_col()
    sql = f"""
    SELECT
        l.ID_LIBRO,
        l.TITULO,
        l.ISBN,
        l.NUM_COPIAS,
        l.NUM_PAGINAS,
        l.ESTADO_FISICO,
        l.CLASIFICACION,
        l.FECHA_REGISTRO,
        l.{col} AS FECHA_PUBLICACION,
        e.NOMBRE AS EDITORIAL,
        g.GENERO AS GENERO,
        i.IDIOMA_LIBRO AS IDIOMA
      FROM LIBRO l
      LEFT JOIN EDITORIAL e ON e.ID_VAREDIT = l.ID_VAREDIT
      LEFT JOIN GENERO g ON g.ID_GENERO = l.ID_GENERO
      LEFT JOIN IDIOMA i ON i.ID_IDIOMA = l.ID_IDIOMA
     ORDER BY l.ID_LIBRO DESC
    """
    return query_all(sql)
