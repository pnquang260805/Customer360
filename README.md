# __Customer 360__

## Overview
Customer 360 sử dụng data lakehouse dựa theo `kiến trúc phân lớp (medallion architecture)`. Mỗi lớp sẽ là 1 bucket và sẽ có các bảng Delta của mình. Các bảng fact và dims sẽ được để trong `silver zone`

+ `Bronze zone`: chứa bảng với các trường `ingested_time`: time stamp, `source`: string, `payload`: string - chứa dữ liệu thô
+ `Silver zone`: chứa các bảng fact và dims
+ `Gold zone`: chứa các bảng đã tổng hợp dữ liệu

## Tech stacks 
- Spark
- Scala
- Python
- Kafka
- Minio
- Delta table

## Yêu cầu
+ Docker
+ Python
+ Scala

## Hướng dẫn sử dụng
```
git clone https://github.com/pnquang260805/Customer360.git
```
```
cd ./Customer360
```

```
docker-compose up -d --build
```

Mở terminal thư mục `pipeline`
```
sbt clean compile build
```

```
docker exec -it master bash

/opt/spark/bin/spark-submit \
  --master spark://master:7077 \
  --executor-cores 1 \
  --total-executor-cores 1 \
  --deploy-mode client \
  --class Main \
  --conf "spark.jars.ivy=/tmp/.ivy2" \
--conf spark.rpc.message.maxSize=1024 \
--conf "spark.network.timeout=800s" \
--conf "spark.rpc.askTimeout=800s" \
--conf "spark.driver.maxResultSize=2g" \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4,io.delta:delta-spark_2.12:3.2.0\
  /opt/spark/work-dir/jars/scala-2.12/pipeline_2.12-0.1.0.jar
```

## Lưu ý
Schema cho các bảng sẽ được thêm sau