#!/bin/bash

set -e
mode=$(echo $MODE | tr '[:upper:]' '[:lower:]')

if [ $mode = "master" ]; then
    echo "Starting master"
    
    /opt/spark/sbin/start-master.sh
    
    echo "Master has been started"
elif [ $mode = "worker" ]; then
    echo "Starting worker"
    # master url: spark://host:post <port default: 7077>
    /opt/spark/sbin/start-worker.sh $MASTER_URL
    
    echo "Worker has been started"
else
    echo "Invalid"
fi
sleep infinity