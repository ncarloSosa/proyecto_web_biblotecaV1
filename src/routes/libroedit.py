"""LibroEdit blueprint."""

from __future__ import annotations

from typing import Dict, List, Tuple

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import login_required

from src.models import autor_dao, editorial_dao, libro_dao, libroedit_dao


bp = Blueprint("libroedit", __name__, url_prefix="/libroedit")


def _paginate(items: List[Dict[str, object]], page: int, per_page: int = 10) -> Tuple[List[Dict[str, object]], int, int]:
    total = len(items)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    return items[start : start + per_page], page, total_pages


def _parse_int(value: str | None, label: str) -> int:
    text = (value or "").strip()
    if not text:
        raise ValueError(f"Debes seleccionar {label}.")
    try:
        return int(text)
    except ValueError as exc:
        raise ValueError(f"El campo {label} es inválido.") from exc


def _build_data(form) -> Dict[str, object]:
    id_editorial = _parse_int(form.get("ID_EDITORIAL"), "una editorial")
    id_libro = _parse_int(form.get("ID_LIBRO"), "un libro")
    id_autor = _parse_int(form.get("ID_AUTOR"), "un autor")
    return {
        "ID_EDITORIAL": id_editorial,
        "ID_LIBRO": id_libro,
        "ID_AUTOR": id_autor,
    }


def _catalogs() -> Dict[str, List[Dict[str, object]]]:
    return {
        "editoriales": editorial_dao.listar(),
        "libros": libro_dao.listar(),
        "autores": autor_dao.listar(),
    }


@bp.get("/")
@login_required
def index():
    page = int(request.args.get("page", 1) or 1)
    registros = libroedit_dao.listar()
    paginated, page, total_pages = _paginate(registros, page)
    return render_template(
        "libroedit/index.html",
        registros=paginated,
        page=page,
        total_pages=total_pages,
    )


@bp.get("/crear")
@login_required
def crear():
    return render_template(
        "libroedit/form.html",
        action=url_for("libroedit.guardar"),
        registro=None,
        **_catalogs(),
    )


@bp.post("/guardar")
@login_required
def guardar():
    try:
        data = _build_data(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("libroedit.crear"))
    libroedit_dao.crear(data)
    flash("Relación creada correctamente.", "success")
    return redirect(url_for("libroedit.index"))


@bp.get("/editar/<int:id_libro_edit>")
@login_required
def editar(id_libro_edit: int):
    registro = libroedit_dao.obtener(id_libro_edit)
    if not registro:
        flash("Relación no encontrada.", "warning")
        return redirect(url_for("libroedit.index"))
    return render_template(
        "libroedit/form.html",
        action=url_for("libroedit.actualizar", id_libro_edit=id_libro_edit),
        registro=registro,
        **_catalogs(),
    )


@bp.post("/actualizar/<int:id_libro_edit>")
@login_required
def actualizar(id_libro_edit: int):
    try:
        data = _build_data(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("libroedit.editar", id_libro_edit=id_libro_edit))
    libroedit_dao.actualizar(id_libro_edit, data)
    flash("Relación actualizada correctamente.", "success")
    return redirect(url_for("libroedit.index"))


@bp.post("/eliminar/<int:id_libro_edit>")
@login_required
def eliminar(id_libro_edit: int):
    libroedit_dao.eliminar(id_libro_edit)
    flash("Relación eliminada.", "info")
    return redirect(url_for("libroedit.index"))
