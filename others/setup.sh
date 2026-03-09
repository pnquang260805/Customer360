#!/bin/bash

until $(curl --output /dev/null --silent --head --fail http://debezium:8083); do
    echo "Đang đợi server..."
    sleep 2
done
python ./main.py