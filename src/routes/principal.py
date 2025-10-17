"""Main menu routes."""
from __future__ import annotations

from flask import Blueprint, render_template
from flask_login import login_required

bp = Blueprint("principal", __name__)


@bp.get("/")
@login_required
def index():
    return render_template("principal/menu.html")
