package transformers

import config.{ConfigVariables, DatalakeConfig}
import interfaces.Transform
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import services._
import utils.TransformUtils

class TransformTransactionSilver(
    hudiService: HudiService,
    mongoDbService: MongoDbService
) extends Transform {
  private val configVars = new ConfigVariables();

  override def stgSilver(df: DataFrame): DataFrame = {
    val dataSchema: StructType = StructType(
      Seq(
        StructField("transaction_id", StringType),
        StructField("customer_id", StringType),
        StructField("product_id", StringType),
        StructField("phone_number", StringType),
        StructField("product_name", StringType),
        StructField("price", StringType),
        StructField("quantity", LongType),
        StructField("total_amount", StringType),
        StructField("event_time", LongType),
        StructField("source", StringType)
      )
    )
    val cdcSchema = StructType(
      Seq(
        // StructField("before", dataSchema, true),
        StructField("after", dataSchema, true),
        // StructField("op", StringType, true),     // 'c' cho create, 'u' cho update, 'd' cho delete
        StructField("ts_ms", LongType, true)
      )
    );

    var rawDf: DataFrame =
      df.where(col("key") === "transaction").select(col("value"));
    var transaction: DataFrame = rawDf.select(
      from_json(col("value").cast(StringType), cdcSchema).alias("after_value")
    );
    transaction = transaction
      .select(
        col("after_value.after.*"),
        (col("after_value.ts_ms") / 1000)
          .cast(TimestampType)
          .alias("timestamp")
      )
      .withWatermark("timestamp", "10 minutes");
    transaction = transaction.filter(col("phone_number").isNotNull);
    transaction = transaction
      .withColumn(
        "price",
        TransformUtils.decodeDecimalUdf(col("price")).cast(DecimalType(10, 2))
      )
      .withColumn(
        "total_amount",
        TransformUtils
          .decodeDecimalUdf(col("total_amount"))
          .cast(DecimalType(10, 2))
      );
    transaction = TransformUtils.normalizePhoneNumber(transaction);
    return transaction;
  }

  def resolve(transactionDf: DataFrame): DataFrame = {
    val customSchema = StructType(
      Array(
        StructField("customer_id", StringType, nullable = true),
        StructField("phone_number", StringType, nullable = true)
      )
    )
    var customerLookup = mongoDbService.readMongo(
      customSchema,
      configVars.ATLAS_DATABASE,
      configVars.ATLAS_COLLECTION,
      configVars.atlasConnectionString()
    );
    customerLookup =
      customerLookup.withColumn("timestamp", current_timestamp());
    var combinedDf = transactionDf
      .as("t")
      .join(
        customerLookup.as("c"),
        expr(s"""|t.phone_number = c.phone_number
            |AND t.timestamp <= c.timestamp + interval 3 minutes
            |""".stripMargin),
        "inner"
      )
    var finalDf = combinedDf
      .select(col("c.customer_id"), col("t.*"))
      .groupBy("c.customer_id")
      .agg(
        collect_list(
          struct(
            "transaction_id",
            "product_id",
            "product_name",
            "price",
            "quantity",
            "total_amount"
          )
        )
      )
      .alias("events")
    return finalDf;
  }

  override def stgGold(df: DataFrame): DataFrame = null
}
