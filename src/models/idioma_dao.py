"""DAO for IDIOMA."""
from __future__ import annotations

from typing import Dict, List, Optional

from .db import execute, query_all, query_one


def _next_id() -> int:
    row = query_one("SELECT NVL(MAX(ID_IDIOMA), 0) + 1 AS ID FROM IDIOMA")
    return int(row["ID"]) if row else 1


def listar() -> List[Dict[str, object]]:
    return query_all("SELECT * FROM IDIOMA ORDER BY ID_IDIOMA DESC")


def obtener(id_idioma: int) -> Optional[Dict[str, object]]:
    return query_one("SELECT * FROM IDIOMA WHERE ID_IDIOMA = :id", {"id": id_idioma})


def crear(data: Dict[str, object]) -> int:
    data = {**data}
    data.setdefault("ID_IDIOMA", _next_id())
    sql = "INSERT INTO IDIOMA (ID_IDIOMA, LENGUA) VALUES (:ID_IDIOMA, :LENGUA)"
    execute(sql, data)
    return int(data["ID_IDIOMA"])


def actualizar(id_idioma: int, data: Dict[str, object]) -> None:
    sql = "UPDATE IDIOMA SET LENGUA = :LENGUA WHERE ID_IDIOMA = :ID_IDIOMA"
    payload = {**data, "ID_IDIOMA": id_idioma}
    execute(sql, payload)


def eliminar(id_idioma: int) -> None:
    execute("DELETE FROM IDIOMA WHERE ID_IDIOMA = :id", {"id": id_idioma})
