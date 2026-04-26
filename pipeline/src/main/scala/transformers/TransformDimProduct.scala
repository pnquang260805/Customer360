package transformers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Transforms silver_product data into gold dim_product with SCD2 fields. Adds
  * product_sk (surrogate key) for the dimension table.
  */
class TransformDimProduct(spark: SparkSession) {

  /** Reads from silver_product and adds SCD2 surrogate key. The actual SCD2
    * merge (expire + insert) is handled by SqlStreamService.
    */
  def stgDimProduct(silverProductDf: DataFrame): DataFrame = {
    // WARNING: Do NOT use expr("uuid()") here.
    // fact_transaction relies on this md5 deterministic generation to resolve
    // surrogate keys when there's a micro-batch race condition.
    silverProductDf
      .withColumn(
        "product_sk",
        md5(concat(col("product_id"), current_date())).cast(StringType)
      )
  }
}
