#!/bin/bash

echo "Start change name"
sed -i -e "s|__S3_ENDPOINT__|${S3_ENDPOINT}|g" \
        -e "s|__S3_SECRET_KEY__|${S3_SECRET_KEY}|g" \
        -e "s|__S3_ACCESS_KEY__|${S3_ACCESS_KEY}|g" \
        -e "s|__POSTGRES_PASSWORD__|${POSTGRES_PASSWORD}|g" \
        -e "s|__POSTGRES_USERNAME__|${POSTGRES_USERNAME}|g" \
        -e "s|__POSTGRES_HOST__|${POSTGRES_HOST}|g" \
        -e "s|__POSTGRES_PORT__|${POSTGRES_PORT}|g" \
        -e "s|__WAREHOUSE__|${WAREHOUSE}|g" /usr/local/metastore/apache-hive-metastore-${HIVE_VERSION}-bin/conf/metastore-site.xml
echo "Done"

# Lệnh này kết nối vào database Postgres và kiểm tra xem schema của Hive đã tồn tại chưa.
# Nếu thành công (Database đã có bảng): Nó trả về mã thoát 0 (thành công). Vì là toán tử || (HOẶC), nếu vế đầu đúng, vế sau sẽ không chạy.
# Nếu thất bại (Database trống rỗng): Nó trả về mã lỗi (khác 0). Lúc này, hệ thống sẽ chuyển sang thực hiện vế 2.
if [ /usr/local/metastore/apache-hive-metastore-${HIVE_VERSION}-bin/bin/schematool -dbType postgres -info ]
then
        echo "Schema already exist"
else
        echo "Init schema"
        /usr/local/metastore/apache-hive-metastore-${HIVE_VERSION}-bin/bin/schematool -initSchema -dbType postgres
        echo "Done"
fi
echo "Starting Metastore..."
exec /usr/local/metastore/apache-hive-metastore-${HIVE_VERSION}-bin/bin/start-metastore