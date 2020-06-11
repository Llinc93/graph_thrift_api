import os
import gevent.monkey
gevent.monkey.patch_all()

import multiprocessing
from cloghandler import ConcurrentRotatingFileHandler

debug = True
loglevel = 'debug'
bind = "0.0.0.0:8140"
pidfile = "gunicorn.pid"
# accesslog = "log/access.log"
# errorlog = "log/debug.log"
daemon = True
timeout = 1800

# 启动的进程数
workers = multiprocessing.cpu_count() - 4
worker_class = 'gevent'
x_forwarded_for_header = 'X-FORWARDED-FOR'

# 日志配置
logconfig_dict = {
    'version':1,
    'disable_existing_loggers': False,
    'loggers':{
        "gunicorn.error": {
            "level": "DEBUG",# 日志的等级
            "handlers": ["error_file"], # handlers里的key
            "propagate": 1,
            "qualname": "gunicorn.error"
        },

        "gunicorn.access": {
            "level": "DEBUG",
            "handlers": ["access_file"],
            "propagate": 1,
            "qualname": "gunicorn.access"
        }
    },
    'handlers':{
        "error_file": {
            "class": "logging.handlers.ConcurrentRotatingFileHandler",
            "maxBytes": 1024 * 1024 * 1024,  #1G  日志文件大小，单位 B
            "backupCount": 5,  # 保留的日志文件数量
            "formatter": "generic",  # 对应formatters里的key
            "filename": "/opt/graph_thrift_api/log/error.log"  # 日志路径
        },
        "access_file": {
            "class": "logging.handlers.ConcurrentRotatingFileHandler",
            "maxBytes": 1024 * 1024 * 1024,
            "backupCount": 5,
            "formatter": "access",
            "filename": "/opt/graph_thrift_api/log/access.log",
        }
    },
    'formatters':{
        "generic": {
            "format": "[%(process)d] %(asctime)s %(levelname)s %(message)s",  # 日志格式
            "datefmt": "[%Y-%m-%d %H:%M:%S]",  # 时间显示方法
            "class": "logging.Formatter"
        },
        "access": {
            "format": "'[%(process)d] %(asctime)s %(levelname)s %(message)s'",
            "class": "logging.Formatter"
        }
    },
    "root": {
       "handlers": ["error_file", "access_file"]
    }
}
