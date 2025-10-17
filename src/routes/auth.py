"""Authentication blueprint."""
from __future__ import annotations

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import LoginManager, current_user, login_required, login_user, logout_user

from src.models import usuario_dao
from src.models.user import User


bp = Blueprint("auth", __name__, url_prefix="/auth")

login_manager = LoginManager()
login_manager.login_view = "auth.login"


@login_manager.user_loader
def load_user(user_id: str) -> User | None:
    try:
        numeric_id = int(user_id)
    except (TypeError, ValueError):
        return None
    record = usuario_dao.obtener(numeric_id)
    return User.from_record(record)


@bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("principal.index"))

    if request.method == "POST":
        nombre = request.form.get("usuario") or request.form.get("username", "")
        nombre = (nombre or "").strip()
        if not nombre:
            flash("Escribe un usuario", "danger")
            return render_template("auth/login.html")

        record = usuario_dao.buscar_por_nombre(nombre)
        if not record:
            flash("Usuario no encontrado", "danger")
            return render_template("auth/login.html")

        form_pwd = request.form.get("password")
        if "PWD" in record and record["PWD"] is not None and form_pwd is not None:
            if not form_pwd or form_pwd != str(record["PWD"]):
                flash("Credenciales inválidas", "danger")
                return render_template("auth/login.html")

        user_id = record.get("ID_USUARIO")
        user_name = record.get("NOMBRE")
        if not user_id or not user_name:
            flash("No se pudo iniciar sesión con el usuario proporcionado.", "danger")
            return render_template("auth/login.html")

        user = User(id=str(user_id), name=user_name, role=record.get("ROL"))

        login_user(user)
        flash("Sesión iniciada correctamente.", "success")
        next_url = request.args.get("next") or url_for("principal.index")
        return redirect(next_url)

    return render_template("auth/login.html")


@bp.get("/logout")
@login_required
def logout():
    logout_user()
    flash("Sesión finalizada.", "info")
    return redirect(url_for("auth.login"))
