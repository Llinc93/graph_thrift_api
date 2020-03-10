#!/bin/bash
#description:This my flask
#chkconfig:2345 20 81
apipath="/opt/graph_thrift_api/main_server.py"

start(){
    python3 $apipath &
    echo 'api start OK'
}

stop(){
    api_pid = `ps -aux | grep "$apipath" | grep -v 'grep' | awk '{print $2}'`
    kill -9 $api_pid
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