package services

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger


class HudiService(spark : SparkSession){
    def createDatabase(dbName : String, location : String): Unit ={
        var query : String = s"CREATE DATABASE IF NOT EXISTS $dbName LOCATION '$location'";
        spark.sql(query);
    }

    def createRawTable(dbName : String, tableName : String, location : String): Unit = {
        var query : String = s"""
            CREATE TABLE IF NOT EXISTS $dbName.$tableName (
                offset STRING,
                topic STRING,
                key STRING,
                value STRING
            )
            USING hudi
            LOCATION '$location' -- External table: table stored in S3 with prop "LOCATION"
        """ 
        spark.sql(query)
    }

    def writeStreamTable(df : DataFrame, checkpoint : String, dbName : String, tableName : String): Unit = {
        df.writeStream.format("hudi")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .trigger(Trigger.ProcessingTime("5 seconds"))
            .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
            .option("hoodie.datasource.hive_sync.enable", "true")
            .option("hoodie.datasource.hive_sync.database", dbName)
            .option("hoodie.datasource.hive_sync.table", tableName)
            .option("hoodie.datasource.hive_sync.mode", "hms") 
            .option("hoodie.datasource.hive_sync.metastore.uris", "thrift://metastore:9083")
            .option("hoodie.datasource.hive_sync.partition_fields", "")
            .option("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.NonPartitionedExtractor")
            .toTable(s"$dbName.$tableName"); // không dùng start

    }
}