import os
import gevent.monkey
gevent.monkey.patch_all()

import multiprocessing

# debug = True
# loglevel = 'debug'
bind = "0.0.0.0:8140"
pidfile = "gunicorn.pid"
accesslog = "log/access.log"
errorlog = "log/debug.log"
daemon = True
timeout = 1800

# 启动的进程数
workers = multiprocessing.cpu_count() - 4
worker_class = 'gevent'
x_forwarded_for_header = 'X-FORWARDED-FOR'