"""Editorial blueprint."""
from __future__ import annotations

from typing import Dict, List, Tuple

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import login_required

from src.models import editorial_dao

bp = Blueprint("editorial", __name__, url_prefix="/editorial")


def _paginate(items: List[Dict[str, object]], page: int, per_page: int = 10) -> Tuple[List[Dict[str, object]], int, int]:
    total = len(items)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    return items[start : start + per_page], page, total_pages


def _trim(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    return value


def _build_data(form) -> Dict[str, object]:
    nombre = _trim(form.get("NOMBRE"))
    pais = _trim(form.get("PAIS"))
    if not nombre:
        raise ValueError("El nombre es obligatorio.")
    return {"NOMBRE": nombre, "PAIS": pais}


@bp.get("/")
@login_required
def index():
    page = int(request.args.get("page", 1) or 1)
    search = request.args.get("q", "").strip().lower()
    editoriales = editorial_dao.listar()
    if search:
        editoriales = [e for e in editoriales if search in (e.get("NOMBRE", "").lower())]
    paginated, page, total_pages = _paginate(editoriales, page)
    return render_template(
        "editorial/index.html",
        editoriales=paginated,
        page=page,
        total_pages=total_pages,
        search=search,
    )


@bp.get("/crear")
@login_required
def crear():
    return render_template("editorial/form.html", action=url_for("editorial.guardar"), editorial=None)


@bp.post("/guardar")
@login_required
def guardar():
    try:
        data = _build_data(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("editorial.crear"))
    editorial_dao.crear(data)
    flash("Editorial creada correctamente.", "success")
    return redirect(url_for("editorial.index"))


@bp.get("/editar/<int:id_varedit>")
@login_required
def editar(id_varedit: int):
    editorial = editorial_dao.obtener(id_varedit)
    if not editorial:
        flash("Editorial no encontrada.", "warning")
        return redirect(url_for("editorial.index"))
    return render_template(
        "editorial/form.html",
        action=url_for("editorial.actualizar", id_varedit=id_varedit),
        editorial=editorial,
    )


@bp.post("/actualizar/<int:id_varedit>")
@login_required
def actualizar(id_varedit: int):
    try:
        data = _build_data(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("editorial.editar", id_varedit=id_varedit))
    editorial_dao.actualizar(id_varedit, data)
    flash("Editorial actualizada correctamente.", "success")
    return redirect(url_for("editorial.index"))


@bp.post("/eliminar/<int:id_varedit>")
@login_required
def eliminar(id_varedit: int):
    editorial_dao.eliminar(id_varedit)
    flash("Editorial eliminada.", "info")
    return redirect(url_for("editorial.index"))
