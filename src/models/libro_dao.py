"""DAO helpers for LIBRO."""

from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, first_existing_column, query_all, query_one


def _pub_col() -> str:
    return first_existing_column(
        "LIBRO",
        ["FECHA_PUBLICACION", "ANO_PUBL", "ANIO_PUBLICACION", "ANO_PUBLICACION"],
    )


def _editorial_fk() -> str:
    return first_existing_column("LIBRO", ["ID_VAREDIT", "NUM_EDITORIAL", "ID_EDITORIAL"])


def _editorial_pk() -> str:
    return first_existing_column("EDITORIAL", ["ID_EDITORIAL", "ID_VAREDIT", "NUM_EDITORIAL"])


def listar() -> List[Dict[str, object]]:
    pub = _pub_col()
    edit_fk = _editorial_fk()
    sql = f"""
    SELECT ID_LIBRO,
           TITULO,
           ISBN,
           {pub} AS FECHA_PUBLICACION,
           CLASIFICACION,
           ESTADO_FISICO,
           FECHA_REGISTRO,
           {edit_fk} AS EDITORIAL_ID,
           ID_GENERO,
           ID_IDIOMA
      FROM LIBRO
     ORDER BY ID_LIBRO DESC
    """
    return query_all(sql)


def obtener(id_libro: int) -> Optional[Dict[str, object]]:
    pub = _pub_col()
    edit_fk = _editorial_fk()
    sql = f"""
    SELECT ID_LIBRO,
           TITULO,
           SUBTITULO,
           ISBN,
           {pub} AS FECHA_PUBLICACION,
           NUM_COPIAS,
           NUM_PAGINAS,
           DESCRIPCION,
           CLASIFICACION,
           PERTENECE_GRUPO,
           ESTADO_FISICO,
           {edit_fk} AS EDITORIAL_ID,
           ID_GENERO,
           ID_IDIOMA
      FROM LIBRO
     WHERE ID_LIBRO = :ID
    """
    return query_one(sql, {"ID": id_libro})


def crear(data: Dict[str, object]) -> None:
    pub = _pub_col()
    edit_fk = _editorial_fk()
    sql = f"""
    INSERT INTO LIBRO
      (ID_LIBRO, TITULO, SUBTITULO, ISBN, {pub}, NUM_COPIAS, NUM_PAGINAS,
       FECHA_REGISTRO, DESCRIPCION, CLASIFICACION, PERTENECE_GRUPO, ESTADO_FISICO,
       {edit_fk}, ID_GENERO, ID_IDIOMA)
    VALUES
      ((SELECT NVL(MAX(ID_LIBRO),0)+1 FROM LIBRO),
       :TITULO, :SUBTITULO, :ISBN, TO_DATE(:FECHA_PUBLICACION,'YYYY-MM-DD'),
       :NUM_COPIAS, :NUM_PAGINAS, SYSDATE, :DESCRIPCION, :CLASIFICACION,
       :PERTENECE_GRUPO, :ESTADO_FISICO, :EDITORIAL_ID, :ID_GENERO, :ID_IDIOMA)
    """
    execute(sql, data)


def actualizar(id_libro: int, data: Dict[str, object]) -> None:
    pub = _pub_col()
    edit_fk = _editorial_fk()
    sql = f"""
    UPDATE LIBRO
       SET TITULO = :TITULO,
           SUBTITULO = :SUBTITULO,
           ISBN = :ISBN,
           {pub} = TO_DATE(:FECHA_PUBLICACION,'YYYY-MM-DD'),
           NUM_COPIAS = :NUM_COPIAS,
           NUM_PAGINAS = :NUM_PAGINAS,
           DESCRIPCION = :DESCRIPCION,
           CLASIFICACION = :CLASIFICACION,
           PERTENECE_GRUPO = :PERTENECE_GRUPO,
           ESTADO_FISICO = :ESTADO_FISICO,
           {edit_fk} = :EDITORIAL_ID,
           ID_GENERO = :ID_GENERO,
           ID_IDIOMA = :ID_IDIOMA
     WHERE ID_LIBRO = :ID
    """
    execute(sql, dict(data, ID=id_libro))


def eliminar(id_libro: int) -> None:
    execute("DELETE FROM LIBRO WHERE ID_LIBRO = :ID", {"ID": id_libro})


def reporte() -> List[Dict[str, object]]:
    pub = _pub_col()
    edit_fk = _editorial_fk()
    ed_pk = _editorial_pk()
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
        l.{pub} AS FECHA_PUBLICACION,
        l.{edit_fk} AS EDITORIAL_ID,
        e.NOMBRE AS EDITORIAL,
        g.GENERO AS GENERO,
        i.IDIOMA_LIBRO AS IDIOMA
      FROM LIBRO l
      LEFT JOIN EDITORIAL e ON e.{ed_pk} = l.{edit_fk}
      LEFT JOIN GENERO g ON g.ID_GENERO = l.ID_GENERO
      LEFT JOIN IDIOMA i ON i.ID_IDIOMA = l.ID_IDIOMA
     ORDER BY l.ID_LIBRO DESC
    """
    return query_all(sql)
