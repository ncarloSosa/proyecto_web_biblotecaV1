"""Authentication blueprint."""
from __future__ import annotations

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import LoginManager, current_user, login_required, login_user, logout_user

from src.models import usuario_dao
from src.models.user import User


bp = Blueprint("auth", __name__)

login_manager = LoginManager()
login_manager.login_view = "auth.login"


@login_manager.user_loader
def load_user(user_id: str) -> User | None:
    try:
        numeric_id = int(user_id)
    except (TypeError, ValueError):
        return None
    record = usuario_dao.obtener(numeric_id)
    if not record:
        return None
    return User(id=str(record["ID_USUARIO"]), nombre=str(record["NOMBRE"]))


@bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("principal.index"))

    if request.method == "GET":
        return render_template("auth/login.html")

    nombre = (request.form.get("username") or "").strip()
    password = (request.form.get("password") or "").strip()

    if not nombre or not password:
        flash("Usuario/contraseña inválidos", "danger")
        return render_template("auth/login.html"), 401

    record = usuario_dao.buscar_por_nombre(nombre)
    if not record or password != str(record.get("CONTRASENA") or ""):
        flash("Usuario/contraseña inválidos", "danger")
        return render_template("auth/login.html"), 401

    user = User(id=str(record["ID_USUARIO"]), nombre=str(record["NOMBRE"]))
    login_user(user)
    return redirect(url_for("principal.index"))


@bp.get("/logout")
@login_required
def logout():
    logout_user()
    flash("Sesión finalizada.", "info")
    return redirect(url_for("auth.login"))
