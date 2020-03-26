#!/bin/bash
#description:This my flask
#chkconfig:2345 20 81
apipath='/opt/graph_thrift_api/main_server.py'
path='/opt/graph_thrift_api'

start(){
    cd $path;nohup python3 $apipath >/dev/null 2>&1 &
    echo 'api start OK'
}

stop(){
    ps -ef | grep "python3 $apipath" | grep -v 'grep' | awk '{print $2}' | xargs kill -9
    echo 'api stop OK'
}


restart(){
    stop
    start
}

case $1 in
    start)
    start
    ;;
    stop)
    stop
    ;;
    restart)
    restart
    ;;
    *)
    start
esac