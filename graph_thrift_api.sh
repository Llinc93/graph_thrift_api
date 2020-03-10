#!/bin/bash
#description:This my flask
#chkconfig:2345 20 81
apipath="/opt/graph_thrift_api/main_server.py"

start(){
    python3 $apipath &
    echo 'api start OK'
}

stop(){
    ps -aux | grep "$apipath" | grep -v 'grep' | awk '{print $2}' | xargs kill -9
    echo 'api stop OK'
}


restart(){
    stop
    echo 'api stop OK'
    start
    echo 'api start OK'
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