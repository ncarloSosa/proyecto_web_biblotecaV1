"""Session user helper."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from flask_login import UserMixin


@dataclass
class User(UserMixin):
    """Simple user representation for flask-login."""

    id: str
    name: str
    role: str | None = None

    @classmethod
    def from_record(cls, record: Mapping[str, Any] | None) -> "User | None":
        if not record:
            return None
        user_id = str(record.get("ID_USUARIO"))
        name = record.get("NOMBRE")
        role = record.get("ROL")
        if not user_id or not name:
            return None
        return cls(id=user_id, name=name, role=role)
