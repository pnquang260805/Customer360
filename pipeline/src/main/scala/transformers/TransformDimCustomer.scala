package transformers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Transforms silver_customer data into gold dim_customer with SCD2 fields.
  * Adds customer_sk (surrogate key) for the dimension table.
  */
class TransformDimCustomer(spark: SparkSession) {

  /** Reads from silver_customer and adds SCD2 surrogate key. The actual SCD2
    * merge (expire + insert) is handled by SqlStreamService.
    */
  def stgDimCustomer(silverCustomerDf: DataFrame): DataFrame = {
    // WARNING: Do NOT use expr("uuid()") here.
    // fact_transaction relies on this md5 deterministic generation to resolve
    // surrogate keys when there's a micro-batch race condition.
    silverCustomerDf
      .withColumn("customer_sk", md5(concat(col("customer_id"), current_date())).cast(StringType))
  }
}
