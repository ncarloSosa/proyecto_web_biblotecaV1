"""LibroEdit blueprint."""

from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Tuple

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import login_required

from src.models import editorial_dao, libro_dao, libroedit_dao


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


def _parse_date(value: str | None) -> str | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("La fecha de edición es inválida.") from exc
    return text


def _first_value(form, names: List[str]) -> str | None:
    for name in names:
        if not name:
            continue
        value = form.get(name)
        if value is not None:
            return value
    return None


def _build_data(form) -> Dict[str, object]:
    id_editorial = _parse_int(
        _first_value(form, ["EDITORIAL_ID", "ID_VAREDIT", "NUM_EDITORIAL", "ID_EDITORIAL"]),
        "una editorial",
    )
    id_libro = _parse_int(
        _first_value(form, ["LIBRO_ID", "LIBRO_ID_LIBRO", "ID_LIBRO"]),
        "un libro",
    )
    fecha_edicion = _parse_date(_first_value(form, ["FECHA", "FECHA_EDICION"]))
    return {
        "EDITORIAL_ID": id_editorial,
        "LIBRO_ID": id_libro,
        "FECHA": fecha_edicion,
    }


def _catalogs() -> Dict[str, List[Dict[str, object]]]:
    return {
        "editoriales": editorial_dao.listar(),
        "libros": libro_dao.listar(),
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


@bp.get("/editar/<int:id_edit_lib>")
@login_required
def editar(id_edit_lib: int):
    registro = libroedit_dao.obtener(id_edit_lib)
    if not registro:
        flash("Relación no encontrada.", "warning")
        return redirect(url_for("libroedit.index"))
    return render_template(
        "libroedit/form.html",
        action=url_for("libroedit.actualizar", id_edit_lib=id_edit_lib),
        registro=registro,
        **_catalogs(),
    )


@bp.post("/actualizar/<int:id_edit_lib>")
@login_required
def actualizar(id_edit_lib: int):
    try:
        data = _build_data(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("libroedit.editar", id_edit_lib=id_edit_lib))
    libroedit_dao.actualizar(id_edit_lib, data)
    flash("Relación actualizada correctamente.", "success")
    return redirect(url_for("libroedit.index"))


@bp.post("/eliminar/<int:id_edit_lib>")
@login_required
def eliminar(id_edit_lib: int):
    libroedit_dao.eliminar(id_edit_lib)
    flash("Relación eliminada.", "info")
    return redirect(url_for("libroedit.index"))
