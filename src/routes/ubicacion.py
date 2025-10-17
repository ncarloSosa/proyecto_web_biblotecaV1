"""Ubicación stub blueprint."""
from __future__ import annotations

from flask import Blueprint, render_template
from flask_login import login_required

bp = Blueprint("ubicacion", __name__, url_prefix="/ubicacion")


@bp.get("/")
@login_required
def index():
    return render_template("ubicacion/index.html")
