package transformers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import interfaces.Transform
import java.util.Base64
import java.math.{BigDecimal, BigInteger}
import utils.TransformUtils

class TransformProductSilver extends Transform {

  def stgSilver(rawDf: DataFrame): DataFrame = {

    // price và base_price là StringType vì Debezium gửi dạng Base64
    val dataSchema = StructType(Seq(
      StructField("product_id",    IntegerType, true),
      StructField("product_name",  StringType,  true),
      StructField("product_link",  StringType,  true),
      StructField("price",         StringType,  true),  // Base64-encoded DECIMAL
      StructField("base_price",    StringType,  true),  // Base64-encoded DECIMAL
      StructField("currency",      StringType,  true),
      StructField("sale_percents", StringType,  true),
      StructField("product_type",  StringType,  true)
    ))

    val cdcSchema = StructType(Seq(
      StructField("after", dataSchema, true)
    ))

    // Bước 1: filter key = "product"
    val rawProduct = rawDf
      .where(col("key") === "product")
      .select("value")

    // Bước 2: parse JSON, làm phẳng
    val explodedDf = rawProduct
      .select(from_json(col("value").cast(StringType), cdcSchema).alias("cdc"))
      .select(col("cdc.after.*"))
      .filter(col("product_id").isNotNull) // bỏ CDC delete (after = null)

    // Bước 3: decode Base64 → DECIMAL, cast product_id
    // No product_sk — that belongs in Gold layer
    explodedDf
      .withColumn("product_id",   col("product_id").cast(StringType))
      .withColumn("price",        TransformUtils.decodeDecimalUdf(col("price")))
      .withColumn("base_price",   TransformUtils.decodeDecimalUdf(col("base_price")))
  }

  def stgGold(silverDf: DataFrame): DataFrame = null
}
