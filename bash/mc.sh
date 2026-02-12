#!/bin/bash

wait_and_alias () {
    echo "Waiting for MinIO..."
    # Lặp lại lệnh set alias cho đến khi thành công
    until /usr/bin/mc alias set minio http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD; do
        echo '...waiting...'
        sleep 1
    done
    echo "MinIO is ready and Alias set."
}

create_policy () {
    echo "create policy"
    # $1 host alias $2 policy name, $3 policy location
    /usr/bin/mc admin policy create $1 $2 $3
}

attach_policy () {
    # $1 host alias, $2 policy name, $3 username
    /usr/bin/mc admin policy attach $1 $2 --user $3
}

create_user () {
    # $1 host alias, $2 access key, $3 secret key
    /usr/bin/mc admin user add $1 $2 $3
}
wait_and_alias
echo "create buckets"
/usr/bin/mc mb --ignore-existing minio/tables;
/usr/bin/mc policy set public minio/tables;
/usr/bin/mc mb --ignore-existing minio/files;
/usr/bin/mc policy set public minio/files;

# create_policy minio airbyte-policy /mymc/airbyte-policy.json

# create_user minio airbyte airbyte-minio
# attach_policy minio airbyte-policy airbyte
# echo "MinIO Setup Complete!"
tail -f /dev/null