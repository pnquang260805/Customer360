package loaders

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.streaming.Trigger

class LoadHudi(spark : SparkSession){
    def loadStreamTable(df : DataFrame, checkpoint : String, dbName : String, tableName : String): Unit = {
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