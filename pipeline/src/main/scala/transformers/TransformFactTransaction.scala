package transformers

import config.DatalakeConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class TransformFactTransaction(spark: SparkSession) {
  private val datalakeConf = new DatalakeConfig()

  private val silverCustomerTable =
    s"${datalakeConf.silverDb}.${datalakeConf.silverCustomerTable}"
  private val silverProductTable =
    s"${datalakeConf.silverDb}.${datalakeConf.dimProduct}"

  /** Stream-static joins to resolve customer_sk and product_sk. Reads silver
    * customer and dim product as static batch snapshots, then joins against the
    * streaming silver transaction DataFrame.
    */
  def stgFact(stgTransDf: DataFrame): DataFrame = {

    // 1. Static read: silver customer (current records only) → customer_sk + phone_number
    val customerDim: DataFrame = spark
      .table(silverCustomerTable)
      .where(col("is_current") === true)
      .select(
        col("customer_sk"),
        col("phone_number").alias("c_phone")
      );

    // 2. Static read: dim product (current records only) → product_sk + product_id
    val productDim: DataFrame = spark
      .table(silverProductTable)
      .where(col("is_current") === true)
      .select(
        col("product_sk"),
        col("product_id").alias("p_product_id")
      );

    // 3. Stream-static join: resolve customer_sk via phone_number
    val withCustomer: DataFrame = stgTransDf
      .join(
        broadcast(customerDim),
        col("phone_number") === col("c_phone"),
        "left"
      )
      .drop("c_phone");

    // 4. Stream-static join: resolve product_sk via product_id
    val withProduct: DataFrame = withCustomer
      .join(
        broadcast(productDim),
        col("product_id") === col("p_product_id"),
        "left"
      )
      .drop("p_product_id");

    // 5. Select final fact columns
    val factDf: DataFrame = withProduct
      .withColumn("transaction_sk", expr("uuid()").cast(StringType))
      .select(
        col("transaction_sk"),
        col("customer_sk"),
        col("product_sk"),
        col("price").cast(DecimalType(10, 2)),
        col("quantity").cast(DecimalType(10, 2)),
        col("total_amount").cast(DecimalType(10, 2)),
        col("timestamp").cast(DateType).alias("timestamp")
      );

    return factDf;
  }
}
