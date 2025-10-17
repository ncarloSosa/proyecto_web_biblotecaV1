"""Authentication blueprint."""
from __future__ import annotations

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import LoginManager, UserMixin, current_user, login_required, login_user, logout_user


bp = Blueprint("auth", __name__, url_prefix="/auth")


class DemoUser(UserMixin):
    def __init__(self, user_id: str, name: str) -> None:
        self.id = user_id
        self.name = name


login_manager = LoginManager()
login_manager.login_view = "auth.login"


@login_manager.user_loader
def load_user(user_id: str) -> DemoUser | None:
    if user_id == "demo":
        return DemoUser("demo", "Demo User")
    return None


@bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("principal.index"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        if not username:
            flash("Debes ingresar un nombre de usuario.", "danger")
        else:
            user = DemoUser("demo", username or "demo")
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
