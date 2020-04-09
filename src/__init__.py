from flask import Flask

from src.controller.mod import MOD


def create_app():
    app = Flask(__name__)

    app.register_blueprint(MOD)
    return app