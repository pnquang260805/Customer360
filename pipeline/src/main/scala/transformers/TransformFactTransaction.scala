package transformers

import config.DatalakeConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import services.SqlStreamService

/** Builds fact_transaction from silver_transaction by resolving surrogate keys
  * from gold dim tables (dim_customer, dim_product, dim_date).
  */
class TransformFactTransaction(spark: SparkSession) {
  private val datalakeConf = new DatalakeConfig()
  private val streamService = new SqlStreamService();

  // Gold layer dimension tables
  private val goldDimCustomerTable =
    s"${datalakeConf.goldDb}.${datalakeConf.dimCustomer}"
  private val goldDimProductTable =
    s"${datalakeConf.goldDb}.${datalakeConf.dimProduct}"
  private val goldDimDateTable =
    s"${datalakeConf.goldDb}.${datalakeConf.dimDate}"

  /** Stream-static joins to resolve customer_sk, product_sk, and date_key.
    * Reads gold dim tables as static batch snapshots, then joins against the
    * streaming silver transaction DataFrame.
    */
  def stgFact(stgTransDf: DataFrame): DataFrame = {
    val newCustomers = stgTransDf
      .where(col("is_new_customer") === true)
      .drop("is_new_customer");

    // streamService.mergeGoldDimCustomer(newCustomers, goldDimCustomerTable);

    // 1. Static read: dim_customer (current records only) → customer_sk + phone_number
    val customerDim: DataFrame = spark
      .table(goldDimCustomerTable)
      .where(col("is_current") === true)
      .select(
        col("customer_sk"),
        col("customer_id").alias("c_customer_id")
      );

    // 2. Static read: dim_product (current records only) → product_sk + product_id
    val productDim: DataFrame = spark
      .table(goldDimProductTable)
      .where(col("is_current") === true)
      .select(
        col("product_sk"),
        col("product_id").alias("p_product_id")
      );

    // 3. Static read: dim_date → date_key + date
    val dateDim: DataFrame = spark
      .table(goldDimDateTable)
      .select(
        col("date_key"),
        col("date").alias("d_date")
      );

    // 4. Stream-static join: resolve customer_sk via phone_number
    val withCustomer: DataFrame = stgTransDf
      .join(
        broadcast(customerDim),
        col("customer_id") === col("c_customer_id"),
        "left"
      )
      .drop("c_customer_id");

    // 5. Stream-static join: resolve product_sk via product_id
    val withProduct: DataFrame = withCustomer
      .join(
        broadcast(productDim),
        col("product_id") === col("p_product_id"),
        "left"
      )
      .drop("p_product_id");

    // 6. Stream-static join: resolve date_key via transaction timestamp date
    val withDate: DataFrame = withProduct
      .join(
        broadcast(dateDim),
        col("timestamp").cast(DateType) === col("d_date"),
        "left"
      )
      .drop("d_date");

    // 7. Select final fact columns
    val factDf: DataFrame = withDate
      .withColumn("transaction_sk", expr("uuid()").cast(StringType))
      // WARNING: Do NOT use expr("uuid()") here!
      // If the stream-static join misses (because the dimension record hasn't committed yet),
      // fact_transaction must generate the EXACT SAME surrogate key that dim_customer will eventually generate.
      // If both generate random UUIDs, they will never match and the fact record will be orphaned!
      .withColumn(
        "customer_sk",
        coalesce(
          col("customer_sk"),
          md5(concat(col("customer_id"), to_date(col("timestamp")).cast(StringType)))
        )
      )
      // WARNING: Do NOT use expr("uuid()") here either!
      .withColumn(
        "product_sk",
        coalesce(
          col("product_sk"),
          md5(concat(col("product_id"), to_date(col("timestamp")).cast(StringType)))
        )
      )
      .select(
        col("transaction_sk"),
        col("date_key"),
        col("customer_sk"),
        col("product_sk"),
        col("customer_id"),
        col("transaction_id"),
        col("price").cast(DecimalType(10, 2)).alias("unit_sales_price"),
        col("quantity").cast(DecimalType(10, 2)),
        col("total_amount").cast(DecimalType(10, 2)),
        col("timestamp").cast(TimestampType).alias("time_stamp")
      );

    return factDf;
  }
}
