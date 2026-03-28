package transformers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import interfaces.Transform
import java.time.LocalDate

class TransformCustomerSilver extends Transform {
  def stgSilver(rawDf: DataFrame): DataFrame = {
    val dataSchema = StructType(Seq(
      StructField("customer_id", StringType, true),
      StructField("first_name", StringType, true),
      StructField("last_name", StringType, true),
      StructField("gender", StringType, true),
      StructField("date_of_birth", IntegerType, true), // Debezium gửi ngày dạng số nguyên (Epoch days)
      StructField("email", StringType, true),
      StructField("phone_number", StringType, true),
      StructField("address", StringType, true),
      StructField("country", StringType, true),
      StructField("creation_date", IntegerType, true)
    ));

    val cdcSchema = StructType(Seq(
      // StructField("before", dataSchema, true),
      StructField("after", dataSchema, true),
       StructField("op", StringType, true),     // 'c' cho create, 'u' cho update, 'd' cho delete
//       StructField("ts_ms", LongType, true)
    ));

    var rawCustomer: DataFrame = rawDf.where(col("key") === "customer").select("value");
    var customerDf: DataFrame = rawCustomer.select(from_json(col("value").cast(StringType), cdcSchema).alias("after_value"));
    var explodedDf: DataFrame = customerDf.select(col("after_value.after.*"), col("after_value.op").alias("op"));
    explodedDf = explodedDf.where(col("op") =!= "d");

    explodedDf = explodedDf.withColumn("phone_number", regexp_replace(col("phone_number"), "[\\(\\)\\s\\-]", ""))
      .withColumn("phone_number", regexp_replace(col("phone_number"), "^\\+84", "0"))

    var droppedCustomer: DataFrame = explodedDf;
    var finalDf: DataFrame = droppedCustomer
      .withColumn("date_of_birth", from_unixtime(col("date_of_birth") * 86400, "yyyy-MM-dd").cast("date"))
      .withColumn("creation_date", from_unixtime(col("creation_date") * 86400, "yyyy-MM-dd").cast("date"))
      .withColumn("customer_sk", uuid().cast("string"))
      .drop("address");
    return finalDf;
  }

  def stgGold(silverDf: DataFrame): DataFrame = {
    return null;
  }

}
