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

  // ── UDF decode Debezium DECIMAL (Base64 → BigDecimal) ─────────────────────
  // Debezium encode kiểu DECIMAL(10,2) của PostgreSQL thành:
  //   - unscaledValue: big-endian signed bytes → encode Base64
  //   - scale được biết trước từ schema (ở đây là 2)
  // Ví dụ: price=800.00 → unscaled=80000 → bytes=[0x01,0x38,0x80] → Base64="ATiA"
  // private val SCALE = 2

  // private val decodeDecimalUdf: UserDefinedFunction = udf[java.math.BigDecimal, String]((base64Str: String) => {
  //   if (base64Str == null) null.asInstanceOf[java.math.BigDecimal]
  //   else {
  //     val bytes      = Base64.getDecoder.decode(base64Str)
  //     val unscaled   = new BigInteger(bytes)           // big-endian signed
  //     new BigDecimal(unscaled, SCALE)
  //   }
  // })

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

    // Bước 3: decode Base64 → DECIMAL, cast product_id, thêm product_sk
    explodedDf
      .withColumn("product_id",   col("product_id").cast(StringType))
      .withColumn("price",        TransformUtils.decodeDecimalUdf(col("price")))
      .withColumn("base_price",   TransformUtils.decodeDecimalUdf(col("base_price")))
      .withColumn("product_sk",   uuid().cast(StringType))
  }

  def stgGold(silverDf: DataFrame): DataFrame = null
}
