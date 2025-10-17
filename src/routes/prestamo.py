"""Préstamo blueprint."""
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Tuple

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import login_required

from src.models import libro_dao, prestamo_dao, usuario_dao

bp = Blueprint("prestamo", __name__, url_prefix="/prestamo")


def _paginate(items: List[Dict[str, object]], page: int, per_page: int = 10) -> Tuple[List[Dict[str, object]], int, int]:
    total = len(items)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    return items[start : start + per_page], page, total_pages


def _trim(value: str | None, length: int | None = None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    if length is not None:
        return value[:length]
    return value


def _parse_date(value: str | None) -> datetime | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("Fecha inválida. Usa el formato YYYY-MM-DD.") from exc


def _parse_int(value: str | None) -> int | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError("Identificador inválido.") from exc


def _form_value(form, *names: str) -> str | None:
    for name in names:
        if name is None:
            continue
        value = form.get(name)
        if value is not None:
            return value
    return None


def _build_data(form) -> Dict[str, object]:
    fecha_prestado = _parse_date(_form_value(form, "FECHA_PRESTADO", "fecha_prestado"))
    if not fecha_prestado:
        raise ValueError("La fecha de préstamo es obligatoria.")
    fecha_caducidad = _parse_date(_form_value(form, "FECHA_CADUCIDAD", "FECH_CADUC", "fecha_caducidad"))
    if not fecha_caducidad:
        raise ValueError("La fecha de caducidad es obligatoria.")
    estado = _trim(_form_value(form, "ESTADO", "estado"), 20)
    estado_fisico = _trim(_form_value(form, "ESTADO_FISICO", "ESTADO_FISIC", "estado_fisico"), 20)
    if not estado or not estado_fisico:
        raise ValueError("Estado y estado físico son obligatorios.")
    id_libro = _parse_int(_form_value(form, "LIBRO_ID_LIBRO", "ID_LIBRO", "libro_id"))
    id_usuario = _parse_int(_form_value(form, "USUARIO_ID_USUARIO", "ID_USUARIO", "usuario_id"))
    if id_libro is None or id_usuario is None:
        raise ValueError("Debes seleccionar libro y usuario.")
    return {
        "FECHA_PRESTADO": fecha_prestado.strftime("%Y-%m-%d"),
        "FECHA_CADUCIDAD": fecha_caducidad.strftime("%Y-%m-%d"),
        "ESTADO": estado,
        "ESTADO_FISICO": estado_fisico,
        "LIBRO_ID_LIBRO": id_libro,
        "USUARIO_ID_USUARIO": id_usuario,
    }


def _load_catalogs() -> Dict[str, List[Dict[str, object]]]:
    return {
        "libros": libro_dao.listar(),
        "usuarios": usuario_dao.listar(),
    }


@bp.get("/")
@login_required
def index():
    page = int(request.args.get("page", 1) or 1)
    search = request.args.get("q", "").strip().lower()
    prestamos = prestamo_dao.listar()
    if search:
        prestamos = [p for p in prestamos if search in (p.get("ESTADO", "").lower())]
    paginated, page, total_pages = _paginate(prestamos, page)
    return render_template(
        "prestamo/index.html",
        prestamos=paginated,
        page=page,
        total_pages=total_pages,
        search=search,
    )


@bp.get("/crear")
@login_required
def crear():
    return render_template(
        "prestamo/form.html",
        action=url_for("prestamo.guardar"),
        prestamo=None,
        **_load_catalogs(),
    )


@bp.post("/guardar")
@login_required
def guardar():
    try:
        data = _build_data(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("prestamo.crear"))
    prestamo_dao.crear(data)
    flash("Préstamo creado correctamente.", "success")
    return redirect(url_for("prestamo.index"))


@bp.get("/editar/<int:id_prestamo>")
@login_required
def editar(id_prestamo: int):
    prestamo = prestamo_dao.obtener(id_prestamo)
    if not prestamo:
        flash("Préstamo no encontrado.", "warning")
        return redirect(url_for("prestamo.index"))
    return render_template(
        "prestamo/form.html",
        action=url_for("prestamo.actualizar", id_prestamo=id_prestamo),
        prestamo=prestamo,
        **_load_catalogs(),
    )


@bp.post("/actualizar/<int:id_prestamo>")
@login_required
def actualizar(id_prestamo: int):
    try:
        data = _build_data(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("prestamo.editar", id_prestamo=id_prestamo))
    prestamo_dao.actualizar(id_prestamo, data)
    flash("Préstamo actualizado correctamente.", "success")
    return redirect(url_for("prestamo.index"))


@bp.post("/eliminar/<int:id_prestamo>")
@login_required
def eliminar(id_prestamo: int):
    prestamo_dao.eliminar(id_prestamo)
    flash("Préstamo eliminado.", "info")
    return redirect(url_for("prestamo.index"))
