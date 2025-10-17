"""Session user helper."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from flask_login import UserMixin


@dataclass
class User(UserMixin):
    """Simple user representation for flask-login."""

    id: str
    nombre: str

    @classmethod
    def from_record(cls, record: Mapping[str, Any] | None) -> "User | None":
        if not record:
            return None
        user_id = record.get("ID_USUARIO")
        nombre = record.get("NOMBRE")
        if user_id is None or not nombre:
            return None
        return cls(id=str(user_id), nombre=str(nombre))
