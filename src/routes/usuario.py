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


def _trim(value: str | None, field: str, required: bool = True) -> str | None:
    text = (value or "").strip()
    if not text:
        if required:
            raise ValueError(f"El campo {field} es obligatorio.")
        return None
    return text


def _parse_date(value: str | None) -> datetime:
    text = (value or "").strip()
    if not text:
        raise ValueError("La fecha de creación es obligatoria.")
    try:
        return datetime.strptime(text, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("Fecha inválida. Usa el formato YYYY-MM-DD.") from exc


def _build_data(form) -> Dict[str, object]:
    usuario_val = _trim(form.get("USUARIO"), "usuario")
    nombre = _trim(form.get("NOMBRE"), "nombre")
    direccion = _trim(form.get("DIRECCION"), "dirección")
    telefono = _trim(form.get("TELEFONO"), "teléfono", required=False)
    dpi = _trim(form.get("DPI"), "DPI")
    sexo = (form.get("SEXO") or "").strip().upper()
    if sexo not in {"M", "F", "O"}:
        raise ValueError("Selecciona un sexo válido (M/F/Otro).")
    fecha_creacion = _parse_date(form.get("FECHA_CREACION"))
    contrasena = _trim(form.get("CONTRASENA"), "contraseña")

    return {
        "USUARIO": usuario_val,
        "NOMBRE": nombre,
        "DIRECCION": direccion,
        "TELEFONO": telefono,
        "DPI": dpi,
        "SEXO": sexo,
        "FECHA_CREACION": fecha_creacion.strftime("%Y-%m-%d"),
        "CONTRASENA": contrasena,
    }


@bp.get("/")
@login_required
def index():
    page = int(request.args.get("page", 1) or 1)
    search = request.args.get("q", "").strip().lower()
    usuarios = usuario_dao.listar()
    if search:
        usuarios = [
            u for u in usuarios if search in (u.get("NOMBRE") or "").lower()
        ]
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
    return render_template(
        "usuario/form.html",
        action=url_for("usuario.guardar"),
        usuario=None,
    )


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
