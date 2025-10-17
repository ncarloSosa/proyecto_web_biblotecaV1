"""Data access helpers for the LIBRO table."""
from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, query_all, query_one


def _next_id() -> int:
    row = query_one("SELECT NVL(MAX(ID_LIBRO), 0) + 1 AS ID FROM LIBRO")
    return int(row["ID"]) if row else 1


def listar() -> List[Dict[str, object]]:
    sql = "SELECT * FROM LIBRO ORDER BY FECHA_REGISTRO DESC"
    return query_all(sql)


def obtener(id_libro: int) -> Optional[Dict[str, object]]:
    sql = "SELECT * FROM LIBRO WHERE ID_LIBRO = :id"
    return query_one(sql, {"id": id_libro})


def crear(data: Dict[str, object]) -> int:
    data = {**data}
    data.setdefault("ID_LIBRO", _next_id())
    sql = (
        "INSERT INTO LIBRO ("
        " ID_LIBRO, TITULO, SUBTITULO, ISBN, ANO_PUBLICACION, NUM_COPIAS,"
        " NUM_PAGINAS, FECHA_REGISTRO, DESCRIPCION, CLASIFICACION,"
        " PERTENECE_GRUPO, ESTADO_FISICO, ID_VAREDIT, ID_GENERO, ID_IDIOMA,"
        " PRESTAMO_ID_PRESTAMO, IDIOMA_ID_IDIOMA"
        ") VALUES ("
        " :ID_LIBRO, :TITULO, :SUBTITULO, :ISBN, :ANO_PUBLICACION, :NUM_COPIAS,"
        " :NUM_PAGINAS, :FECHA_REGISTRO, :DESCRIPCION, :CLASIFICACION,"
        " :PERTENECE_GRUPO, :ESTADO_FISICO, :ID_VAREDIT, :ID_GENERO, :ID_IDIOMA,"
        " :PRESTAMO_ID_PRESTAMO, :IDIOMA_ID_IDIOMA"
        ")"
    )
    execute(sql, data)
    return int(data["ID_LIBRO"])


def actualizar(id_libro: int, data: Dict[str, object]) -> None:
    sql = (
        "UPDATE LIBRO SET "
        "TITULO = :TITULO, SUBTITULO = :SUBTITULO, ISBN = :ISBN, "
        "ANO_PUBLICACION = :ANO_PUBLICACION, NUM_COPIAS = :NUM_COPIAS, "
        "NUM_PAGINAS = :NUM_PAGINAS, FECHA_REGISTRO = :FECHA_REGISTRO, "
        "DESCRIPCION = :DESCRIPCION, CLASIFICACION = :CLASIFICACION, "
        "PERTENECE_GRUPO = :PERTENECE_GRUPO, ESTADO_FISICO = :ESTADO_FISICO, "
        "ID_VAREDIT = :ID_VAREDIT, ID_GENERO = :ID_GENERO, ID_IDIOMA = :ID_IDIOMA, "
        "PRESTAMO_ID_PRESTAMO = :PRESTAMO_ID_PRESTAMO, IDIOMA_ID_IDIOMA = :IDIOMA_ID_IDIOMA "
        "WHERE ID_LIBRO = :ID_LIBRO"
    )
    payload = {**data, "ID_LIBRO": id_libro}
    execute(sql, payload)


def eliminar(id_libro: int) -> None:
    execute("DELETE FROM LIBRO WHERE ID_LIBRO = :id", {"id": id_libro})


def reporte() -> List[Dict[str, object]]:
    sql = """
        SELECT
            l.ID_LIBRO,
            l.TITULO,
            l.ISBN,
            l.NUM_COPIAS,
            l.NUM_PAGINAS,
            l.ESTADO_FISICO,
            l.CLASIFICACION,
            l.FECHA_REGISTRO,
            e.NOMBRE AS EDITORIAL,
            g.GENERO AS GENERO,
            i.IDIOMA_LIBRO AS IDIOMA
        FROM LIBRO l
        LEFT JOIN EDITORIAL e ON e.ID_EDITORIAL = l.ID_VAREDIT
        LEFT JOIN GENERO g ON g.ID_GENERO = l.ID_GENERO
        LEFT JOIN IDIOMA i ON i.ID_IDIOMA = l.ID_IDIOMA
        ORDER BY l.FECHA_REGISTRO DESC
    """
    return query_all(sql)
