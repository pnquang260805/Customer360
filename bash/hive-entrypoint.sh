#!/bin/bash

if [ /opt/hive/bin/schematool -dbType postgres -info ]
then
        echo "Schema already exist"
else
        echo "Init schema"
        /opt/hive/bin/schematool -initSchema -dbType postgres
        echo "Done"
fi
echo "Starting Metastore..."
exec /opt/hive/bin/hive --service $SERVICE_NAME