"""Historial blueprint."""

from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Tuple

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import login_required

from src.models import historial_dao, libro_dao, usuario_dao


bp = Blueprint("historial", __name__, url_prefix="/historial")


def _paginate(items: List[Dict[str, object]], page: int, per_page: int = 10) -> Tuple[List[Dict[str, object]], int, int]:
    total = len(items)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    return items[start : start + per_page], page, total_pages


def _parse_date(value: str | None) -> datetime:
    text = (value or "").strip()
    if not text:
        raise ValueError("La fecha es obligatoria.")
    try:
        return datetime.strptime(text, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("Fecha inválida. Usa el formato YYYY-MM-DD.") from exc


def _parse_int(value: str | None, label: str) -> int:
    text = (value or "").strip()
    if not text:
        raise ValueError(f"Debes seleccionar {label}.")
    try:
        return int(text)
    except ValueError as exc:
        raise ValueError(f"El campo {label} es inválido.") from exc


def _build_data(form) -> Dict[str, object]:
    tip_mov = (form.get("TIP_MOV") or "").strip()
    if not tip_mov:
        raise ValueError("El tipo de movimiento es obligatorio.")
    fecha = _parse_date(form.get("FECHA"))
    usuario_id = _parse_int(form.get("USUARIO_ID_USUARIO"), "un usuario")
    libro_id = _parse_int(form.get("LIBRO_ID_LIBRO"), "un libro")
    return {
        "TIP_MOV": tip_mov,
        "FECHA": fecha.strftime("%Y-%m-%d"),
        "USUARIO_ID_USUARIO": usuario_id,
        "LIBRO_ID_LIBRO": libro_id,
    }


def _catalogs() -> Dict[str, List[Dict[str, object]]]:
    return {
        "usuarios": usuario_dao.listar(),
        "libros": libro_dao.listar(),
    }


@bp.get("/")
@login_required
def index():
    page = int(request.args.get("page", 1) or 1)
    registros = historial_dao.listar()
    paginated, page, total_pages = _paginate(registros, page)
    return render_template(
        "historial/index.html",
        registros=paginated,
        page=page,
        total_pages=total_pages,
    )


@bp.get("/crear")
@login_required
def crear():
    return render_template(
        "historial/form.html",
        action=url_for("historial.guardar"),
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
        return redirect(url_for("historial.crear"))
    historial_dao.crear(data)
    flash("Movimiento registrado correctamente.", "success")
    return redirect(url_for("historial.index"))


@bp.get("/editar/<int:id_historial>")
@login_required
def editar(id_historial: int):
    registro = historial_dao.obtener(id_historial)
    if not registro:
        flash("Movimiento no encontrado.", "warning")
        return redirect(url_for("historial.index"))
    return render_template(
        "historial/form.html",
        action=url_for("historial.actualizar", id_historial=id_historial),
        registro=registro,
        **_catalogs(),
    )


@bp.post("/actualizar/<int:id_historial>")
@login_required
def actualizar(id_historial: int):
    try:
        data = _build_data(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("historial.editar", id_historial=id_historial))
    historial_dao.actualizar(id_historial, data)
    flash("Movimiento actualizado correctamente.", "success")
    return redirect(url_for("historial.index"))


@bp.post("/eliminar/<int:id_historial>")
@login_required
def eliminar(id_historial: int):
    historial_dao.eliminar(id_historial)
    flash("Movimiento eliminado.", "info")
    return redirect(url_for("historial.index"))
