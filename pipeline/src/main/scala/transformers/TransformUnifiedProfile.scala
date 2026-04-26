package transformers

import config.{ConfigVariables, DatalakeConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Builds the unified_profile document for MongoDB serving layer.
  * Combines gold dim_customer + fact_transaction + event activity_log.
  * 
  * unified_profile schema:
  *   - master_id (= customer_id, business key)
  *   - first_name, last_name, gender, date_of_birth
  *   - transaction_log (array of transaction records)
  *   - activity_log (array of event records)
  */
class TransformUnifiedProfile(spark: SparkSession) {
  private val datalakeConf = new DatalakeConfig()

  private val goldDimCustomerTable =
    s"${datalakeConf.goldDb}.${datalakeConf.dimCustomer}"
  private val goldFactTransactionTable =
    s"${datalakeConf.goldDb}.${datalakeConf.factTransaction}"

  /** Builds unified profile by joining dim_customer with fact_transaction.
    * Returns a DataFrame with customer demographics + transaction_log.
    * Should be called in a foreachBatch context.
    */
  def buildProfile(
      customerDf: DataFrame,
      transactionDf: DataFrame,
      eventDf: DataFrame
  ): DataFrame = {
    // Customer demographics (current records from dim_customer)
    val customerProfile = customerDf.select(
      col("customer_id").alias("master_id"),
      col("first_name"),
      col("last_name"),
      col("gender"),
      col("date_of_birth")
    )

    // Transaction log: aggregate transactions by customer
    val transactionLog = transactionDf
      .groupBy("customer_id")
      .agg(
        collect_list(
          struct(
            "transaction_id",
            "unit_sales_price",
            "quantity",
            "total_amount",
            "time_stamp"
          )
        ).alias("transaction_log")
      )
      .withColumnRenamed("customer_id", "t_customer_id")

    // Join customer + transactions
    var profileDf = customerProfile
      .join(
        transactionLog,
        col("master_id") === col("t_customer_id"),
        "left"
      )
      .drop("t_customer_id")

    // If event data is provided, add activity_log
    if (eventDf != null) {
      profileDf = profileDf
        .join(
          eventDf,
          col("master_id") === eventDf("customer_id"),
          "left"
        )
        .drop(eventDf("customer_id"))
        .withColumnRenamed("collect_list(struct(event_id, type, url, time_stamp))", "activity_log")
    }

    // Fill nulls for log columns
    profileDf
  }
}
