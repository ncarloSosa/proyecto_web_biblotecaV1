"""Libro blueprint."""
from __future__ import annotations

import csv
import io
from datetime import date, datetime
from typing import Dict, List, Tuple

from flask import Blueprint, Response, flash, redirect, render_template, request, url_for
from flask_login import login_required

from src.models import editorial_dao, genero_dao, idioma_dao, libro_dao
from src.utils.filters import shortdate

bp = Blueprint("libro", __name__, url_prefix="/libro")


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


def _parse_int(value: str | None) -> int | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError("Debes ingresar un número válido.") from exc


def _parse_date(value: str | None, allow_year: bool = False) -> date | None:
    value = (value or "").strip()
    if not value:
        return None
    if allow_year and len(value) == 4 and value.isdigit():
        return date(int(value), 1, 1)
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("Fecha inválida. Usa el formato YYYY-MM-DD o solo el año.") from exc


def _build_data(form) -> Dict[str, object]:
    titulo = _trim(form.get("TITULO"), 20)
    if not titulo:
        raise ValueError("El título es obligatorio.")
    subtitulo = _trim(form.get("SUBTITULO"), 20)
    isbn = _trim(form.get("ISBN"), 20)
    fecha_publicacion = _parse_date(form.get("FECHA_PUBLICACION"), allow_year=True)
    num_copias = _parse_int(form.get("NUM_COPIAS"))
    num_paginas = _parse_int(form.get("NUM_PAGINAS"))
    descripcion = _trim(form.get("DESCRIPCION"), 200)
    clasificacion = _trim(form.get("CLASIFICACION"), 20)
    if not clasificacion:
        raise ValueError("La clasificación es obligatoria.")
    pertenece_grupo = (form.get("PERTENECE_GRUPO", "N").strip() or "N").upper()
    if pertenece_grupo not in {"S", "N"}:
        pertenece_grupo = "N"
    estado_fisico = _trim(form.get("ESTADO_FISICO"), 20)
    if not estado_fisico:
        raise ValueError("El estado físico es obligatorio.")

    def _optional_fk(name: str) -> int | None:
        return _parse_int(form.get(name))

    return {
        "TITULO": titulo,
        "SUBTITULO": subtitulo,
        "ISBN": isbn,
        "FECHA_PUBLICACION": fecha_publicacion.strftime("%Y-%m-%d") if fecha_publicacion else None,
        "NUM_COPIAS": num_copias,
        "NUM_PAGINAS": num_paginas,
        "DESCRIPCION": descripcion,
        "CLASIFICACION": clasificacion,
        "PERTENECE_GRUPO": pertenece_grupo,
        "ESTADO_FISICO": estado_fisico,
        "ID_VAREDIT": _optional_fk("ID_VAREDIT"),
        "ID_GENERO": _optional_fk("ID_GENERO"),
        "ID_IDIOMA": _optional_fk("ID_IDIOMA"),
    }


def _load_catalogs() -> Dict[str, List[Dict[str, object]]]:
    return {
        "editoriales": editorial_dao.listar(),
        "generos": genero_dao.listar(),
        "idiomas": idioma_dao.listar(),
    }


@bp.get("/")
@login_required
def index():
    page = int(request.args.get("page", 1) or 1)
    search = request.args.get("q", "").strip().lower()
    libros = libro_dao.listar()
    if search:
        libros = [l for l in libros if search in (l.get("TITULO", "").lower())]
    paginated, page, total_pages = _paginate(libros, page)
    return render_template(
        "libro/index.html",
        libros=paginated,
        page=page,
        total_pages=total_pages,
        search=search,
    )


@bp.get("/crear")
@login_required
def crear():
    return render_template("libro/form.html", action=url_for("libro.guardar"), libro=None, **_load_catalogs())


@bp.post("/guardar")
@login_required
def guardar():
    try:
        data = _build_data(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("libro.crear"))
    libro_dao.crear(data)
    flash("Libro creado correctamente.", "success")
    return redirect(url_for("libro.index"))


@bp.get("/editar/<int:id_libro>")
@login_required
def editar(id_libro: int):
    libro = libro_dao.obtener(id_libro)
    if not libro:
        flash("Libro no encontrado.", "warning")
        return redirect(url_for("libro.index"))
    return render_template(
        "libro/form.html",
        action=url_for("libro.actualizar", id_libro=id_libro),
        libro=libro,
        **_load_catalogs(),
    )


@bp.post("/actualizar/<int:id_libro>")
@login_required
def actualizar(id_libro: int):
    try:
        data = _build_data(request.form)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("libro.editar", id_libro=id_libro))
    libro_dao.actualizar(id_libro, data)
    flash("Libro actualizado correctamente.", "success")
    return redirect(url_for("libro.index"))


@bp.post("/eliminar/<int:id_libro>")
@login_required
def eliminar(id_libro: int):
    libro_dao.eliminar(id_libro)
    flash("Libro eliminado.", "info")
    return redirect(url_for("libro.index"))


@bp.get("/reporte")
@login_required
def reporte():
    data = libro_dao.reporte()
    return render_template("libro/reporte.html", libros=data)


@bp.get("/reporte.csv")
@login_required
def reporte_csv():
    data = libro_dao.reporte()
    output = io.StringIO()
    writer = csv.writer(output)
    headers = [
        "ID_LIBRO",
        "TITULO",
        "ISBN",
        "NUM_COPIAS",
        "NUM_PAGINAS",
        "ESTADO_FISICO",
        "CLASIFICACION",
        "FECHA_PUBLICACION",
        "FECHA_REGISTRO",
        "EDITORIAL",
        "GENERO",
        "IDIOMA",
    ]
    writer.writerow(headers)
    for row in data:
        values = []
        for col in headers:
            value = row.get(col)
            if col in {"FECHA_REGISTRO", "FECHA_PUBLICACION"}:
                value = shortdate(value)
            values.append("" if value is None else value)
        writer.writerow(values)
    csv_data = output.getvalue()
    response = Response(csv_data, mimetype="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=libros.csv"
    return response
