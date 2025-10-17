"""DAO for GRUPO_LECTURA and related tables."""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from .db import execute, next_id, query_all, query_one


def _next_sequence(sequence: str) -> int:
    row = query_one(f"SELECT {sequence}.NEXTVAL AS ID FROM dual")
    return int(row["ID"]) if row else next_id("GRUPO_LECTURA", "ID_GRUPO")


def listar() -> List[Dict[str, object]]:
    sql = """
        SELECT ID_GRUPO, NOMBRE, DESCRIPCION, FECHA_REUNION, HORA_REUNION, LUGAR, ID_LIBGRUP
          FROM GRUPO_LECTURA
         ORDER BY FECHA_REUNION DESC
    """
    return query_all(sql)


def obtener(id_grupo: int) -> Optional[Dict[str, object]]:
    sql = """
        SELECT ID_GRUPO, NOMBRE, DESCRIPCION, FECHA_REUNION, HORA_REUNION, LUGAR, ID_LIBGRUP
          FROM GRUPO_LECTURA
         WHERE ID_GRUPO = :ID
    """
    return query_one(sql, {"ID": id_grupo})


def _insert_libros(id_libgrup: int, lista_ids: Iterable[int]) -> None:
    execute("DELETE FROM LIBRO_GRUPO WHERE ID_LIBGRUP = :ID", {"ID": id_libgrup})
    for libro_id in lista_ids:
        execute(
            "INSERT INTO LIBRO_GRUPO (ID_LIBGRUP, ID_LIBRO) VALUES (:ID_LIBGRUP, :ID_LIBRO)",
            {"ID_LIBGRUP": id_libgrup, "ID_LIBRO": int(libro_id)},
        )


def crear(data: Dict[str, object], libros_ids: Iterable[int]) -> int:
    id_libgrup = next_id("LIBRO_GRUPO", "ID_LIBGRUP")
    _insert_libros(id_libgrup, libros_ids)

    grupo_id = _next_sequence("GRUPO_LECT_SEQ")
    payload = {
        **data,
        "ID_GRUPO": grupo_id,
        "ID_LIBGRUP": id_libgrup,
    }
    sql = """
        INSERT INTO GRUPO_LECTURA
          (ID_GRUPO, NOMBRE, DESCRIPCION, FECHA_REUNION, HORA_REUNION, LUGAR, ID_LIBGRUP)
        VALUES
          (:ID_GRUPO, :NOMBRE, :DESCRIPCION, TO_DATE(:FECHA_REUNION,'YYYY-MM-DD'), :HORA_REUNION, :LUGAR, :ID_LIBGRUP)
    """
    execute(sql, payload)
    return grupo_id


def actualizar(id_grupo: int, data: Dict[str, object], libros_ids: Iterable[int]) -> None:
    payload = {
        **data,
        "ID_GRUPO": id_grupo,
    }
    sql = """
        UPDATE GRUPO_LECTURA
           SET NOMBRE = :NOMBRE,
               DESCRIPCION = :DESCRIPCION,
               FECHA_REUNION = TO_DATE(:FECHA_REUNION,'YYYY-MM-DD'),
               HORA_REUNION = :HORA_REUNION,
               LUGAR = :LUGAR
         WHERE ID_GRUPO = :ID_GRUPO
    """
    execute(sql, payload)

    grupo = obtener(id_grupo)
    if grupo and grupo.get("ID_LIBGRUP") is not None:
        _insert_libros(int(grupo["ID_LIBGRUP"]), libros_ids)


def eliminar(id_grupo: int) -> None:
    grupo = obtener(id_grupo)
    if grupo and grupo.get("ID_LIBGRUP") is not None:
        execute("DELETE FROM LIBRO_GRUPO WHERE ID_LIBGRUP = :ID", {"ID": grupo["ID_LIBGRUP"]})
    execute("DELETE FROM GRUPO_LECTURA WHERE ID_GRUPO = :ID", {"ID": id_grupo})


def listar_libros(id_grupo: int) -> List[Dict[str, object]]:
    grupo = obtener(id_grupo)
    if not grupo or grupo.get("ID_LIBGRUP") is None:
        return []
    sql = """
        SELECT LG.ID_LIBRO, L.TITULO
          FROM LIBRO_GRUPO LG
          JOIN LIBRO L ON L.ID_LIBRO = LG.ID_LIBRO
         WHERE LG.ID_LIBGRUP = :ID
         ORDER BY L.TITULO
    """
    return query_all(sql, {"ID": grupo["ID_LIBGRUP"]})


def reemplazar_libros(id_libgrup: int, lista_ids: Iterable[int]) -> None:
    _insert_libros(id_libgrup, lista_ids)
