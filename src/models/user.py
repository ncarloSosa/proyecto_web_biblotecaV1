"""Session user helper."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from flask_login import UserMixin


@dataclass
class User(UserMixin):
    """Simple user representation for flask-login."""

    id: str
    username: str
    name: str

    @classmethod
    def from_record(cls, record: Mapping[str, Any] | None) -> "User | None":
        if not record:
            return None
        user_id = record.get("ID_USUARIO")
        username = record.get("USUARIO")
        name = record.get("NOMBRE") or record.get("USUARIO")
        if user_id is None or not username or not name:
            return None
        return cls(id=str(user_id), username=str(username), name=name)
