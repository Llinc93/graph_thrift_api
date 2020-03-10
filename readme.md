
# 说明

demon java代码示例
---

# thrift

+ java:  
```
thrift --gen java java.thrift
```

+ python:  
``` 
thrift --gen py interface.thrift
```
---

+ python依赖  
```
pip3 install -r requirements.txt  
````
+ python thrift主程序 
``` 
  python3 main_server.py
```
+ python thrift客户端示例
``` 
  python3 client.py
```

+ API 自启动
```
  chmod +x graph_thrift_api.sh
  cp graph_thrift_api.sh /etc/init.d/
  chkconfig --add graph_thrift_api.sh
  chkconfig --list
```

+ API 启动/停止/重启
```
  ./graph_thrift_api.sh start/stop/restart 
```