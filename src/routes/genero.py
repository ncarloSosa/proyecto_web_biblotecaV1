"""Género blueprint."""
from __future__ import annotations

from typing import Dict, List, Tuple

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import login_required

from src.models import genero_dao, libro_dao

bp = Blueprint("genero", __name__, url_prefix="/genero")


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
    genero = _trim(form.get("GENERO"))
    libro_id = form.get("LIBRO_ID_LIBRO")
    if not genero:
        raise ValueError("El nombre del género es obligatorio.")
    if not libro_id:
        raise ValueError("Debes seleccionar un libro asociado.")
    try:
        libro_id_int = int(libro_id)
    except ValueError as exc:
        raise ValueError("Identificador de libro inválido.") from exc
    return {"GENERO": genero, "LIBRO_ID_LIBRO": libro_id_int}


@bp.get("/")
@login_required
def index():
    page = int(request.args.get("page", 1) or 1)
    search = request.args.get("q", "").strip().lower()
    generos = genero_dao.listar()
    if search:
        generos = [g for g in generos if search in (g.get("GENERO", "").lower())]
    paginated, page, total_pages = _paginate(generos, page)
    return render_template(
        "genero/index.html",
        generos=paginated,
        page=page,
        total_pages=total_pages,
        search=search,
    )


@bp.get("/crear")
@login_required
def crear():
    return render_template(
        "genero/form.html",
        action=url_for("genero.guardar"),
        genero=None,
        libros=libro_dao.listar(),
    )


@bp.post("/guardar")
@login_required
def guardar():
    try:
        data = _build_data(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("genero.crear"))
    genero_dao.crear(data)
    flash("Género creado correctamente.", "success")
    return redirect(url_for("genero.index"))


@bp.get("/editar/<int:id_genero>")
@login_required
def editar(id_genero: int):
    genero = genero_dao.obtener(id_genero)
    if not genero:
        flash("Género no encontrado.", "warning")
        return redirect(url_for("genero.index"))
    return render_template(
        "genero/form.html",
        action=url_for("genero.actualizar", id_genero=id_genero),
        genero=genero,
        libros=libro_dao.listar(),
    )


@bp.post("/actualizar/<int:id_genero>")
@login_required
def actualizar(id_genero: int):
    try:
        data = _build_data(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("genero.editar", id_genero=id_genero))
    genero_dao.actualizar(id_genero, data)
    flash("Género actualizado correctamente.", "success")
    return redirect(url_for("genero.index"))


@bp.post("/eliminar/<int:id_genero>")
@login_required
def eliminar(id_genero: int):
    genero_dao.eliminar(id_genero)
    flash("Género eliminado.", "info")
    return redirect(url_for("genero.index"))
