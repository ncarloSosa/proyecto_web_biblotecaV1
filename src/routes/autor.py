"""Autor blueprint."""
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Tuple

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import login_required

from src.models import autor_dao

bp = Blueprint("autor", __name__, url_prefix="/autor")


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
        raise ValueError("Fecha de nacimiento inválida.") from exc


def _build_data(form) -> Dict[str, object]:
    nombre = _trim(form.get("NOMBRE"))
    apellido = _trim(form.get("APELLIDO"))
    if not nombre or not apellido:
        raise ValueError("Nombre y apellido son obligatorios.")
    fechanac = _parse_date(form.get("FECH_NACIMIENT"))
    nacionalidad = _trim(form.get("NACIONALIDAD"))
    biografia = _trim(form.get("BIOGRAFIA"), 50)
    return {
        "NOMBRE": nombre,
        "APELLIDO": apellido,
        "FECH_NACIMIENT": fechanac,
        "NACIONALIDAD": nacionalidad,
        "BIOGRAFIA": biografia,
    }


@bp.get("/")
@login_required
def index():
    page = int(request.args.get("page", 1) or 1)
    search = request.args.get("q", "").strip().lower()
    autores = autor_dao.listar()
    if search:
        autores = [a for a in autores if search in (a.get("NOMBRE", "") + " " + a.get("APELLIDO", "")).lower()]
    paginated, page, total_pages = _paginate(autores, page)
    return render_template(
        "autor/index.html",
        autores=paginated,
        page=page,
        total_pages=total_pages,
        search=search,
    )


@bp.get("/crear")
@login_required
def crear():
    return render_template("autor/form.html", action=url_for("autor.guardar"), autor=None)


@bp.post("/guardar")
@login_required
def guardar():
    try:
        data = _build_data(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("autor.crear"))
    autor_dao.crear(data)
    flash("Autor creado correctamente.", "success")
    return redirect(url_for("autor.index"))


@bp.get("/editar/<int:id_autor>")
@login_required
def editar(id_autor: int):
    autor = autor_dao.obtener(id_autor)
    if not autor:
        flash("Autor no encontrado.", "warning")
        return redirect(url_for("autor.index"))
    return render_template("autor/form.html", action=url_for("autor.actualizar", id_autor=id_autor), autor=autor)


@bp.post("/actualizar/<int:id_autor>")
@login_required
def actualizar(id_autor: int):
    try:
        data = _build_data(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("autor.editar", id_autor=id_autor))
    autor_dao.actualizar(id_autor, data)
    flash("Autor actualizado correctamente.", "success")
    return redirect(url_for("autor.index"))


@bp.post("/eliminar/<int:id_autor>")
@login_required
def eliminar(id_autor: int):
    autor_dao.eliminar(id_autor)
    flash("Autor eliminado.", "info")
    return redirect(url_for("autor.index"))
