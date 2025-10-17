"""Miembro blueprint."""

from __future__ import annotations

from typing import Dict, List, Tuple

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import login_required

from src.models import grupo_lectura_dao, miembro_dao, usuario_dao


bp = Blueprint("miembro", __name__, url_prefix="/miembro")


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
    id_usuario = _parse_int(form.get("ID_USUARIO"), "un usuario")
    id_grupo = _parse_int(form.get("ID_GRUPO"), "un grupo")
    return {
        "ID_USUARIO": id_usuario,
        "ID_GRUPO": id_grupo,
    }


def _catalogs() -> Dict[str, List[Dict[str, object]]]:
    return {
        "usuarios": usuario_dao.listar(),
        "grupos": grupo_lectura_dao.listar(),
    }


@bp.get("/")
@login_required
def index():
    page = int(request.args.get("page", 1) or 1)
    registros = miembro_dao.listar()
    paginated, page, total_pages = _paginate(registros, page)
    return render_template(
        "miembro/index.html",
        registros=paginated,
        page=page,
        total_pages=total_pages,
    )


@bp.get("/crear")
@login_required
def crear():
    return render_template(
        "miembro/form.html",
        action=url_for("miembro.guardar"),
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
        return redirect(url_for("miembro.crear"))
    miembro_dao.crear(data)
    flash("Miembro registrado correctamente.", "success")
    return redirect(url_for("miembro.index"))


@bp.get("/editar/<int:id_miembro>")
@login_required
def editar(id_miembro: int):
    registro = miembro_dao.obtener(id_miembro)
    if not registro:
        flash("Miembro no encontrado.", "warning")
        return redirect(url_for("miembro.index"))
    return render_template(
        "miembro/form.html",
        action=url_for("miembro.actualizar", id_miembro=id_miembro),
        registro=registro,
        **_catalogs(),
    )


@bp.post("/actualizar/<int:id_miembro>")
@login_required
def actualizar(id_miembro: int):
    try:
        data = _build_data(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("miembro.editar", id_miembro=id_miembro))
    miembro_dao.actualizar(id_miembro, data)
    flash("Miembro actualizado correctamente.", "success")
    return redirect(url_for("miembro.index"))


@bp.post("/eliminar/<int:id_miembro>")
@login_required
def eliminar(id_miembro: int):
    miembro_dao.eliminar(id_miembro)
    flash("Miembro eliminado.", "info")
    return redirect(url_for("miembro.index"))
