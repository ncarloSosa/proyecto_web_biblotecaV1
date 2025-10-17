"""Ubicación blueprint."""

from __future__ import annotations

from typing import Dict, List, Tuple

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import login_required

from src.models import ubicacion_dao


bp = Blueprint("ubicacion", __name__, url_prefix="/ubicacion")


def _paginate(items: List[Dict[str, object]], page: int, per_page: int = 10) -> Tuple[List[Dict[str, object]], int, int]:
    total = len(items)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    return items[start : start + per_page], page, total_pages


def _trim(value: str | None, field: str, required: bool = True) -> str | None:
    text = (value or "").strip()
    if not text:
        if required:
            raise ValueError(f"El campo {field} es obligatorio.")
        return None
    return text


def _build_data(form) -> Dict[str, object]:
    estanteria = _trim(form.get("ESTANTERIA"), "estantería")
    descripcion = _trim(form.get("DESCRIPCION"), "descripción", required=False)
    return {
        "ESTANTERIA": estanteria,
        "DESCRIPCION": descripcion,
    }


@bp.get("/")
@login_required
def index():
    page = int(request.args.get("page", 1) or 1)
    registros = ubicacion_dao.listar()
    paginated, page, total_pages = _paginate(registros, page)
    return render_template(
        "ubicacion/index.html",
        registros=paginated,
        page=page,
        total_pages=total_pages,
    )


@bp.get("/crear")
@login_required
def crear():
    return render_template(
        "ubicacion/form.html",
        action=url_for("ubicacion.guardar"),
        registro=None,
    )


@bp.post("/guardar")
@login_required
def guardar():
    try:
        data = _build_data(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("ubicacion.crear"))
    ubicacion_dao.crear(data)
    flash("Ubicación creada correctamente.", "success")
    return redirect(url_for("ubicacion.index"))


@bp.get("/editar/<int:id_ubicacion>")
@login_required
def editar(id_ubicacion: int):
    registro = ubicacion_dao.obtener(id_ubicacion)
    if not registro:
        flash("Ubicación no encontrada.", "warning")
        return redirect(url_for("ubicacion.index"))
    return render_template(
        "ubicacion/form.html",
        action=url_for("ubicacion.actualizar", id_ubicacion=id_ubicacion),
        registro=registro,
    )


@bp.post("/actualizar/<int:id_ubicacion>")
@login_required
def actualizar(id_ubicacion: int):
    try:
        data = _build_data(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("ubicacion.editar", id_ubicacion=id_ubicacion))
    ubicacion_dao.actualizar(id_ubicacion, data)
    flash("Ubicación actualizada correctamente.", "success")
    return redirect(url_for("ubicacion.index"))


@bp.post("/eliminar/<int:id_ubicacion>")
@login_required
def eliminar(id_ubicacion: int):
    ubicacion_dao.eliminar(id_ubicacion)
    flash("Ubicación eliminada.", "info")
    return redirect(url_for("ubicacion.index"))
