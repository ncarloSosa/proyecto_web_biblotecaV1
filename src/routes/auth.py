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
        username = request.form.get("usuario") or request.form.get("username", "")
        username = (username or "").strip()
        password = (request.form.get("password") or "").strip()

        if not username or not password:
            flash("Escribe usuario y contraseña", "danger")
            return render_template("auth/login.html")

        record = usuario_dao.buscar_por_nombre(username)
        if not record:
            flash("Usuario o contraseña inválidos", "danger")
            return render_template("auth/login.html")

        stored_password = record.get("CONTRASENA")
        if stored_password is None or str(stored_password).strip() != password:
            flash("Usuario o contraseña inválidos", "danger")
            return render_template("auth/login.html")

        display_name = record.get("NOMBRE") or record.get("USUARIO")
        user = User(
            id=str(record["ID_USUARIO"]),
            username=str(record["USUARIO"]),
            name=str(display_name),
        )

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
