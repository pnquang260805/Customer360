package services

import org.apache.spark.sql.SparkSession

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
}