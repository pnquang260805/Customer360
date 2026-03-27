# __Customer 360__

## Overview
Customer 360 sử dụng data lakehouse dựa theo `kiến trúc phân lớp (medallion architecture)`. Mỗi lớp sẽ là 1 folder và sẽ có các bảng Delta của mình. Các bảng fact và dims sẽ được để trong `silver zone`

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
sbt clean compile package
```

```
docker exec -it master bash

/opt/spark/bin/spark-submit \
  --master spark://master:7077 \
  --deploy-mode client \
  --class Main \
  --conf "spark.jars.ivy=/tmp/.ivy2" \
  --conf spark.rpc.message.maxSize=1024 \
  --conf "spark.network.timeout=800s" \
  --conf "spark.rpc.askTimeout=800s" \
  --conf "spark.driver.maxResultSize=2g" \
  --driver-class-path "/opt/spark/work-dir/jars/logback-classic-1.2.11.jar:/opt/spark/work-dir/jars/logback-core-1.2.11.jar" \
  --conf "spark.driver.extraJavaOptions=-Dlogback.configurationFile=logback.xml" \
  --conf "spark.driver.extraJavaOptions=-Dlogback.configurationFile=logback.xml -Dlogback.debug=true" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.6.0,com.typesafe.scala-logging:scala-logging_2.12:3.9.5,ch.qos.logback:logback-classic:1.2.11" \
  --jars /opt/spark/work-dir/jar_files/hadoop-aws-3.3.4.jar,/opt/spark/work-dir/jar_files/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/work-dir/jar_files/hudi-spark3.5-bundle_2.12-1.0.0.jar \
  /opt/spark/work-dir/jars/scala-2.12/pipeline_2.12-0.1.0.jar
```

## Lưu ý
Schema cho các bảng sẽ được thêm sau
