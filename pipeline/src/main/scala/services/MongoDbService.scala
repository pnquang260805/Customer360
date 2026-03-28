package services

import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import config.ConfigVariables

class MongoDbService(spark: SparkSession) {
  private val configVars = new ConfigVariables;

  def writeMongoDb(df: DataFrame, database: String, collection: String, connectionString: String, idColumn: String, checkpoint: String): Unit = {
    // MongoDB Spark Connector (phiên bản V2/Spark 3) không hỗ trợ Output Mode là Update trực tiếp từ phương thức writeStream.
    df.writeStream.foreachBatch(
        (batchDf: DataFrame, batchId: Long) => {
          batchDf.write.format("mongodb")
            .option("forceDeleteTempCheckpointLocation", "true")
            .option("spark.mongodb.connection.uri", connectionString)
            .option("spark.mongodb.database", database)
            .option("spark.mongodb.collection", collection)
            .option("idFieldList", idColumn)
            .option("operationType", "update")
            .option("hoodie.cleaner.commits.retained", "3")
            .option("hoodie.metadata.enable", "false")
            .mode(SaveMode.Append)
            .save();
        }

      )
      .option("checkpointLocation", checkpoint)
      .option("hoodie.metadata.enable", "false")
      .outputMode("update")
      .start();
  }
}
