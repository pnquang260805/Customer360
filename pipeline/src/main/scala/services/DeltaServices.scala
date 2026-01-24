package services

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row


class DeltaServices(spark : SparkSession){
    def createDatabase(dbName : String, location : String): Unit ={
        var query : String = s"CREATE DATABASE IF NOT EXISTS $dbName LOCATION '$location'";
        spark.sql(query);
    }

    def createRawTable(dbName : String, tableName : String, location : String): Unit = {
        var query : String = s"""
            CREATE TABLE IF NOT EXISTS $dbName.$tableName (
                ingested_time TIMESTAMP,
                source STRING,
                payload STRING
            )
            USING DELTA
            PARTITIONED BY (source)
            LOCATION '$location'
        """
        spark.sql(query)
    }
}