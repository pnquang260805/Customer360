package services

import org.apache.spark.sql.SparkSession
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

    def createSilverTransaction(dbName : String, tableName : String, location : String): Unit = {
        var query : String = s"""
            CREATE TABLE IF NOT EXISTS $dbName.$tableName (
                transaction_id STRING,
                customer_id STRING,
                product_id STRING,
                product_name STRING,
                price DECIMAL(10,2),
                quantity DECIMAL(10,2),
                total_amount DECIMAL(10, 2),
                event_time STRING
            )
            USING hudi
            TBLPROPERTIES (
                type = 'mor', -- Merge on read
                primaryKey = 'transaction_id',
                orderingFields = 'event_time',
                recordMergeMode = 'EVENT_TIME_ORDERING'
            )
            LOCATION '$location' -- External table: table stored in S3 with prop "LOCATION"
        """ 
        spark.sql(query)
    }

    def createSilverCustomer(dbName : String, tableName : String, location : String): Unit = {
        var query : String = s"""
            CREATE TABLE IF NOT EXISTS $dbName.$tableName (
                -- customer_sk STRING,
                customer_id STRING,
                first_name STRING,
                last_name STRING,
                gender STRING,
                date_of_birth DATE,
                email STRING,
                phone_number STRING,
                country STRING,
                creation_date DATE,
                effective_date DATE,
                expired_date DATE,
                is_current BOOLEAN
            )
            USING hudi
            TBLPROPERTIES (
                type = 'mor', -- Merge on read
                primaryKey = 'customer_id',
                precombineField = 'creation_date'
            )
            LOCATION '$location' -- External table: table stored in S3 with prop "LOCATION"
        """ 
        spark.sql(query)
    }

    def writeStream(df : DataFrame, checkpoint : String, dbName : String, tableName : String, tablePath : String): Unit = {
        df.writeStream.format("hudi")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .trigger(Trigger.ProcessingTime("3 seconds"))
            .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
            .option("hoodie.datasource.hive_sync.enable", "true")
            .option("hoodie.datasource.hive_sync.database", dbName)
            .option("hoodie.datasource.hive_sync.table", tableName)
            .option("hoodie.datasource.hive_sync.mode", "hms") 
            .option("hoodie.datasource.hive_sync.metastore.uris", "thrift://metastore:9083")
            .option("hoodie.datasource.hive_sync.partition_fields", "")
            .option("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.NonPartitionedExtractor")
            .option("hoodie.table.name", tableName)
            .toTable(s"$dbName.$tableName"); // không dùng start
            // .start(tablePath)
    }

    def readStreamTable(tablePath : String) : DataFrame = {
        var streamDf = spark.readStream.format("hudi")
                            .load(tablePath);
        return streamDf;
    } 
}