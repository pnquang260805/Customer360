package transformers

import interfaces.Transform
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DecimalType, LongType, StringType, StructField, StructType}

class TransformTransactionSilver extends Transform {

  override def stgSilver(df: DataFrame): DataFrame = {
    val dataSchema: StructType = StructType(Seq(
      StructField("transaction_id", StringType),
      StructField("customer_id", StringType),
      StructField("product_id", StringType),
      StructField("product_name", StringType),
      StructField("price", DecimalType(10, 2)),
      StructField("quantity", LongType),
      StructField("total_amount", DecimalType(10, 2)),
      StructField("event_time", LongType),
      StructField("source", StringType)
    ))
    val cdcSchema = StructType(Seq(
      // StructField("before", dataSchema, true),
      StructField("after", dataSchema, true),
      // StructField("op", StringType, true),     // 'c' cho create, 'u' cho update, 'd' cho delete
      StructField("ts_ms", LongType, true)
    ));
    return null;
  }

  override def stgGold(df: DataFrame): DataFrame = null
}
