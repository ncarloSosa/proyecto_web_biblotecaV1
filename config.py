"""Application configuration utilities."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv
import os


_BASE_DIR = Path(__file__).resolve().parent
load_dotenv(_BASE_DIR / ".env")


def _get_env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    if value is None:
        return None
    return value.strip()


@dataclass
class Config:
    """Configuration values loaded from environment variables."""

    ORACLE_USER: str | None = _get_env("ORACLE_USER")
    ORACLE_PASSWORD: str | None = _get_env("ORACLE_PASSWORD")
    ORACLE_DSN: str | None = _get_env("ORACLE_DSN")
    ORACLE_POOL_MIN: int = int(_get_env("ORACLE_POOL_MIN", "1") or 1)
    ORACLE_POOL_MAX: int = int(_get_env("ORACLE_POOL_MAX", "5") or 5)
    SECRET_KEY: str = _get_env("SECRET_KEY", "change-me") or "change-me"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "ORACLE_USER": self.ORACLE_USER,
            "ORACLE_PASSWORD": self.ORACLE_PASSWORD,
            "ORACLE_DSN": self.ORACLE_DSN,
            "ORACLE_POOL_MIN": self.ORACLE_POOL_MIN,
            "ORACLE_POOL_MAX": self.ORACLE_POOL_MAX,
            "SECRET_KEY": self.SECRET_KEY,
        }


def load_config(app: Any) -> Config:
    """Attach configuration values to the given Flask app."""

    config = Config()
    app.config.update(config.as_dict())
    app.secret_key = config.SECRET_KEY
    return config
