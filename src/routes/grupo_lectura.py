"""Grupo de lectura blueprint."""
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Tuple

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import login_required

from src.models import grupo_lectura_dao, libro_dao

bp = Blueprint("grupo_lectura", __name__, url_prefix="/grupo_lectura")


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


def _parse_date(value: str | None) -> datetime | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("Fecha inválida. Usa el formato YYYY-MM-DD.") from exc


def _parse_time(value: str | None) -> datetime | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        parsed_time = datetime.strptime(value, "%H:%M").time()
        # Oracle DATE necesita un datetime completo; usamos fecha base 1900-01-01
        return datetime.combine(datetime(1900, 1, 1), parsed_time)
    except ValueError as exc:
        raise ValueError("Hora inválida. Usa el formato HH:MM.") from exc


def _build_data(form) -> Dict[str, object]:
    nombre = _trim(form.get("NOMBRE"))
    descripcion = _trim(form.get("DESCRIPCION"))
    fecha_reunion = _parse_date(form.get("FECHA_REUNION"))
    hora_reunion = _parse_time(form.get("HORA_REUNION"))
    lugar = _trim(form.get("LUGAR"))
    if not nombre or not fecha_reunion or not hora_reunion or not lugar:
        raise ValueError("Nombre, fecha, hora y lugar son obligatorios.")
    return {
        "NOMBRE": nombre,
        "DESCRIPCION": descripcion,
        "FECHA_REUNION": fecha_reunion,
        "HORA_REUNION": hora_reunion,
        "LUGAR": lugar,
    }


def _selected_libros(form) -> List[int]:
    values = form.getlist("LIBROS")
    result: List[int] = []
    for value in values:
        value = value.strip()
        if not value:
            continue
        try:
            result.append(int(value))
        except ValueError:
            continue
    return result


@bp.get("/")
@login_required
def index():
    page = int(request.args.get("page", 1) or 1)
    grupos = grupo_lectura_dao.listar()
    paginated, page, total_pages = _paginate(grupos, page)
    return render_template(
        "grupo_lectura/index.html",
        grupos=paginated,
        page=page,
        total_pages=total_pages,
    )


@bp.get("/crear")
@login_required
def crear():
    return render_template(
        "grupo_lectura/form.html",
        action=url_for("grupo_lectura.guardar"),
        grupo=None,
        libros=libro_dao.listar(),
        seleccionados=[],
    )


@bp.post("/guardar")
@login_required
def guardar():
    try:
        data = _build_data(request.form)
        libros = _selected_libros(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("grupo_lectura.crear"))
    grupo_id = grupo_lectura_dao.crear(data)
    grupo_lectura_dao.reemplazar_libros(grupo_id, libros)
    flash("Grupo creado correctamente.", "success")
    return redirect(url_for("grupo_lectura.index"))


@bp.get("/editar/<int:id_grupo>")
@login_required
def editar(id_grupo: int):
    grupo = grupo_lectura_dao.obtener(id_grupo)
    if not grupo:
        flash("Grupo no encontrado.", "warning")
        return redirect(url_for("grupo_lectura.index"))
    seleccionados = [item["ID_LIBRO"] for item in grupo_lectura_dao.listar_libros(id_grupo)]
    return render_template(
        "grupo_lectura/form.html",
        action=url_for("grupo_lectura.actualizar", id_grupo=id_grupo),
        grupo=grupo,
        libros=libro_dao.listar(),
        seleccionados=seleccionados,
    )


@bp.post("/actualizar/<int:id_grupo>")
@login_required
def actualizar(id_grupo: int):
    try:
        data = _build_data(request.form)
        libros = _selected_libros(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("grupo_lectura.editar", id_grupo=id_grupo))
    grupo_lectura_dao.actualizar(id_grupo, data)
    grupo_lectura_dao.reemplazar_libros(id_grupo, libros)
    flash("Grupo actualizado correctamente.", "success")
    return redirect(url_for("grupo_lectura.index"))


@bp.post("/eliminar/<int:id_grupo>")
@login_required
def eliminar(id_grupo: int):
    grupo_lectura_dao.eliminar(id_grupo)
    grupo_lectura_dao.reemplazar_libros(id_grupo, [])
    flash("Grupo eliminado.", "info")
    return redirect(url_for("grupo_lectura.index"))
