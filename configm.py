import os
import gevent.monkey
gevent.monkey.patch_all()

import multiprocessing
# from cloghandler import ConcurrentRotatingFileHandler

debug = True
bind = "0.0.0.0:8140"
pidfile = "gunicorn.pid"
daemon = True
timeout = 1800

# 启动的进程数
workers = multiprocessing.cpu_count() - 4
worker_class = 'gevent'
x_forwarded_for_header = 'X-FORWARDED-FOR'
