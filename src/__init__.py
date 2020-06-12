import os
from flask import Flask
from logging.config import dictConfig
from cloghandler import ConcurrentRotatingFileHandler

from src.controller.mod import MOD



def create_app():
    path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_file = os.path.join(path, "log/access.log")
    log_dict = {
        'version': 1,
        'root': {
            # handler中的level会覆盖掉这里的level
            'level': 'DEBUG',
            'handlers': ['access_file']
        },

        'handlers': {
            'wsgi': {
                'class': 'logging.StreamHandler',
                'stream': 'ext://flask.logging.wsgi_errors_stream',
                'formatter': 'default'
            },
            "access_file": {
                "class": "logging.handlers.ConcurrentRotatingFileHandler",
                "maxBytes": 1024 * 1024 * 1024,  # 打日志的大小，单位字节，这种写法是1G
                "backupCount": 1,  # 保留日志数量
                "encoding": "utf-8",
                "level": "INFO",
                "formatter": "default",  # 对应下面的键
                "filename": log_file  # 打日志的路径
            },
        },
        'formatters': {
            'default': {
                'format': '[%(asctime)s] %(levelname)s  %(message)s',
            },
            'simple': {
                'format': '%(asctime)s - %(levelname)s - %(message)s'
            }
        },
    }
    dictConfig(log_dict)

    app = Flask(__name__)
    app.register_blueprint(MOD)

    return app