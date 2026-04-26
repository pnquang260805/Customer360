package services

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import config.ConfigVariables
import org.apache.spark.sql.types.StructType

class MongoDbService(spark: SparkSession) {
  private val configVars = new ConfigVariables;

  def writeMongoDb(
      df: DataFrame,
      database: String,
      collection: String,
      connectionString: String,
      idColumn: String,
      checkpoint: String
  ): Unit = {
    // MongoDB Spark Connector (phiên bản V2/Spark 3) không hỗ trợ Output Mode là Update trực tiếp từ phương thức writeStream.
    df.writeStream
      .foreachBatch((batchDf: DataFrame, batchId: Long) => {
        batchDf.write
          .format("mongodb")
          .option("forceDeleteTempCheckpointLocation", "true")
          .option("spark.mongodb.connection.uri", connectionString)
          .option("spark.mongodb.database", database)
          .option("spark.mongodb.collection", collection)
          .option("idFieldList", idColumn)
          .option("operationType", "update")
          .option(
            "partitioner",
            "com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner"
          )
          .mode(SaveMode.Append)
          .save();
      })
      .option("checkpointLocation", checkpoint)
      .option("hoodie.metadata.enable", "false")
      .outputMode("update")
      .start();
    print("=====Inserted to mongodb=====")
  }

  def readMongo(
      schema: StructType,
      database: String,
      collection: String,
      connectionString: String
  ): DataFrame = {
    return spark.read
      .format("mongodb")
      .option("spark.mongodb.connection.uri", connectionString)
      .option("spark.mongodb.database", database)
      .option("spark.mongodb.collection", collection)
      .option("partitioner", "com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner")
      .schema(schema)
      .load()
  }
}
