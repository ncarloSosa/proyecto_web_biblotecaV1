"""Editorial blueprint."""
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Tuple

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import login_required

from src.models import editorial_dao

bp = Blueprint("editorial", __name__, url_prefix="/editorial")


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


def _build_data(form) -> Dict[str, object]:
    nombre = _trim(form.get("NOMBRE"))
    pais = _trim(form.get("PAIS"))
    ano_text = (form.get("ANO_EDICION") or "").strip()
    if not ano_text:
        raise ValueError("El año de edición es obligatorio.")
    if not (ano_text.isdigit() or _is_iso_date(ano_text)):
        raise ValueError("El año de edición debe ser YYYY o YYYY-MM-DD.")
    num_editorial = None
    num_text = (form.get("NUM_EDITORIAL") or "").strip()
    if num_text:
        try:
            num_editorial = int(num_text)
        except ValueError as exc:
            raise ValueError("El número editorial debe ser numérico.") from exc
    if not nombre:
        raise ValueError("El nombre es obligatorio.")
    return {
        "NOMBRE": nombre,
        "PAIS": pais,
        "ANO_EDICION": ano_text,
        "NUM_EDITORIAL": num_editorial,
    }


def _is_iso_date(value: str) -> bool:
    try:
        datetime.strptime(value[:10], "%Y-%m-%d")
    except ValueError:
        return False
    return True


@bp.get("/")
@login_required
def index():
    page = int(request.args.get("page", 1) or 1)
    search = request.args.get("q", "").strip().lower()
    editoriales = editorial_dao.listar()
    if search:
        editoriales = [e for e in editoriales if search in (e.get("NOMBRE", "").lower())]
    paginated, page, total_pages = _paginate(editoriales, page)
    return render_template(
        "editorial/index.html",
        editoriales=paginated,
        page=page,
        total_pages=total_pages,
        search=search,
    )


@bp.get("/crear")
@login_required
def crear():
    return render_template("editorial/form.html", action=url_for("editorial.guardar"), editorial=None)


@bp.post("/guardar")
@login_required
def guardar():
    try:
        data = _build_data(request.form)
        editorial_dao.crear(data)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("editorial.crear"))
    flash("Editorial creada correctamente.", "success")
    return redirect(url_for("editorial.index"))


@bp.get("/editar/<int:id_editorial>")
@login_required
def editar(id_editorial: int):
    editorial = editorial_dao.obtener(id_editorial)
    if not editorial:
        flash("Editorial no encontrada.", "warning")
        return redirect(url_for("editorial.index"))
    return render_template(
        "editorial/form.html",
        action=url_for("editorial.actualizar", id_editorial=id_editorial),
        editorial=editorial,
    )


@bp.post("/actualizar/<int:id_editorial>")
@login_required
def actualizar(id_editorial: int):
    try:
        data = _build_data(request.form)
        editorial_dao.actualizar(id_editorial, data)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("editorial.editar", id_editorial=id_editorial))
    flash("Editorial actualizada correctamente.", "success")
    return redirect(url_for("editorial.index"))


@bp.post("/eliminar/<int:id_editorial>")
@login_required
def eliminar(id_editorial: int):
    editorial_dao.eliminar(id_editorial)
    flash("Editorial eliminada.", "info")
    return redirect(url_for("editorial.index"))
