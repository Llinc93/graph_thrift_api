
# 说明


+ API 启动
```
  gunicorn -c gunicorn_conf.py main:app
```

+ API 停止
```
ps -ef|grep 'gunicorn -c gunicorn_conf.py main:app'| awk '{print $2}'| xwargs kill -9
```