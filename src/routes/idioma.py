"""Idioma blueprint."""
from __future__ import annotations

from typing import Dict, List, Tuple

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import login_required

from src.models import idioma_dao

bp = Blueprint("idioma", __name__, url_prefix="/idioma")


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
    lengua = _trim(form.get("IDIOMA_LIBRO") or form.get("LENGUA"))
    if not lengua:
        raise ValueError("El nombre del idioma es obligatorio.")
    return {"IDIOMA_LIBRO": lengua}


@bp.get("/")
@login_required
def index():
    page = int(request.args.get("page", 1) or 1)
    search = request.args.get("q", "").strip().lower()
    idiomas = idioma_dao.listar()
    if search:
        idiomas = [i for i in idiomas if search in (i.get("IDIOMA_LIBRO", "").lower())]
    paginated, page, total_pages = _paginate(idiomas, page)
    return render_template(
        "idioma/index.html",
        idiomas=paginated,
        page=page,
        total_pages=total_pages,
        search=search,
    )


@bp.get("/crear")
@login_required
def crear():
    return render_template("idioma/form.html", action=url_for("idioma.guardar"), idioma=None)


@bp.post("/guardar")
@login_required
def guardar():
    try:
        data = _build_data(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("idioma.crear"))
    idioma_dao.crear(data)
    flash("Idioma creado correctamente.", "success")
    return redirect(url_for("idioma.index"))


@bp.get("/editar/<int:id_idioma>")
@login_required
def editar(id_idioma: int):
    idioma = idioma_dao.obtener(id_idioma)
    if not idioma:
        flash("Idioma no encontrado.", "warning")
        return redirect(url_for("idioma.index"))
    return render_template("idioma/form.html", action=url_for("idioma.actualizar", id_idioma=id_idioma), idioma=idioma)


@bp.post("/actualizar/<int:id_idioma>")
@login_required
def actualizar(id_idioma: int):
    try:
        data = _build_data(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("idioma.editar", id_idioma=id_idioma))
    idioma_dao.actualizar(id_idioma, data)
    flash("Idioma actualizado correctamente.", "success")
    return redirect(url_for("idioma.index"))


@bp.post("/eliminar/<int:id_idioma>")
@login_required
def eliminar(id_idioma: int):
    idioma_dao.eliminar(id_idioma)
    flash("Idioma eliminado.", "info")
    return redirect(url_for("idioma.index"))
