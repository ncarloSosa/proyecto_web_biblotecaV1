"""Usuario blueprint."""
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Tuple

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import login_required

from src.models import usuario_dao

bp = Blueprint("usuario", __name__, url_prefix="/usuario")


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


def _build_data(form) -> Dict[str, object]:
    nombre = _trim(form.get("NOMBRE"))
    rol = _trim(form.get("ROL"))
    fecha_registro = _parse_date(form.get("FECHA_REGISTRO"))
    if not nombre or not rol or not fecha_registro:
        raise ValueError("Nombre, rol y fecha de registro son obligatorios.")
    return {
        "NOMBRE": nombre,
        "ROL": rol,
        "FECHA_REGISTRO": fecha_registro,
    }


@bp.get("/")
@login_required
def index():
    page = int(request.args.get("page", 1) or 1)
    search = request.args.get("q", "").strip().lower()
    usuarios = usuario_dao.listar()
    if search:
        usuarios = [u for u in usuarios if search in (u.get("NOMBRE", "").lower())]
    paginated, page, total_pages = _paginate(usuarios, page)
    return render_template(
        "usuario/index.html",
        usuarios=paginated,
        page=page,
        total_pages=total_pages,
        search=search,
    )


@bp.get("/crear")
@login_required
def crear():
    return render_template("usuario/form.html", action=url_for("usuario.guardar"), usuario=None)


@bp.post("/guardar")
@login_required
def guardar():
    try:
        data = _build_data(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("usuario.crear"))
    usuario_dao.crear(data)
    flash("Usuario creado correctamente.", "success")
    return redirect(url_for("usuario.index"))


@bp.get("/editar/<int:id_usuario>")
@login_required
def editar(id_usuario: int):
    usuario = usuario_dao.obtener(id_usuario)
    if not usuario:
        flash("Usuario no encontrado.", "warning")
        return redirect(url_for("usuario.index"))
    return render_template(
        "usuario/form.html",
        action=url_for("usuario.actualizar", id_usuario=id_usuario),
        usuario=usuario,
    )


@bp.post("/actualizar/<int:id_usuario>")
@login_required
def actualizar(id_usuario: int):
    try:
        data = _build_data(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("usuario.editar", id_usuario=id_usuario))
    usuario_dao.actualizar(id_usuario, data)
    flash("Usuario actualizado correctamente.", "success")
    return redirect(url_for("usuario.index"))


@bp.post("/eliminar/<int:id_usuario>")
@login_required
def eliminar(id_usuario: int):
    usuario_dao.eliminar(id_usuario)
    flash("Usuario eliminado.", "info")
    return redirect(url_for("usuario.index"))
