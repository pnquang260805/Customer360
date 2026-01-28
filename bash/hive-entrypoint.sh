#!/bin/bash

if [ "$SERVICE_NAME" = "metastore" ]; then
        if /opt/hive-metastore/bin/schematool -dbType postgres -info > /dev/null 2>&1
                then
                        echo "Metastore schema already initialized."
                else
                        echo "Initializing metastore schema..."
                        /opt/hive-metastore/bin/schematool --verbose -initSchema -dbType postgres -driver org.postgresql.Driver  \
                                -userName "$HIVE_POSTGRES_USERNAME" -passWord "$HIVE_POSTGRES_PASSWORD" \
                                -url "jdbc:postgresql://postgres-hive:5432/metastore?user=$HIVE_POSTGRES_USERNAME&password=$HIVE_POSTGRES_PASSWORD&createDatabaseIfNotExist=true"
                        echo "Schema initialization completed."
        fi
fi

echo "Starting $SERVICE_NAME..."
exec /opt/hive-metastore/bin/start-metastore
echo "Done $SERVICE_NAME"