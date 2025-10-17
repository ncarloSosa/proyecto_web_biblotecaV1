"""Application entry point for the biblioteca project."""
from __future__ import annotations

from flask import Flask, redirect, url_for

from config import load_config
from src.routes import autor, editorial, genero, grupo_lectura, historial, idioma, libro, libroedit, miembro, prestamo, principal, ubicacion, usuario
from src.routes.auth import bp as auth_bp, login_manager
from src.utils.filters import shortdate


def create_app() -> Flask:
    app = Flask(__name__, template_folder="src/templates", static_folder="src/static")
    load_config(app)

    login_manager.init_app(app)
    app.jinja_env.filters["shortdate"] = shortdate

    app.register_blueprint(auth_bp)
    app.register_blueprint(principal.bp)
    app.register_blueprint(libro.bp)
    app.register_blueprint(autor.bp)
    app.register_blueprint(editorial.bp)
    app.register_blueprint(genero.bp)
    app.register_blueprint(idioma.bp)
    app.register_blueprint(prestamo.bp)
    app.register_blueprint(usuario.bp)
    app.register_blueprint(grupo_lectura.bp)
    app.register_blueprint(historial.bp)
    app.register_blueprint(libroedit.bp)
    app.register_blueprint(miembro.bp)
    app.register_blueprint(ubicacion.bp)

    @app.route("/")
    def root_redirect():
        return redirect(url_for("principal.index"))

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)
