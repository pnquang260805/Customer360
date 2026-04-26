package services
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.LocalDate
import config.ConfigVariables
import java.util.UUID

class SqlStreamService {
  val configVars = new ConfigVariables();

  private def createUpdateColumns(cols: Seq[String]): String = {
    cols.map(c => s"$c = source.$c").mkString(",");
  }

  private def getNow(): String = {
    var now: LocalDate = LocalDate.now();
    return now.toString();
  }

  private def genSk(): String = {
    return UUID.randomUUID().toString();
  }

  // ── SILVER LAYER (Simple upsert — no SCD2) ──────────────────────────────

  /** Upsert into silver_customer (MERGE by customer_id) */
  def upsertSilverCustomer(
      customerDf: DataFrame,
      customerTable: String
  ): Unit = {
    val customerTempView: String = "customerSilverStg";
    val batchProcess: (DataFrame, Long) => Unit =
      (batchDf: DataFrame, batchId: Long) => {
        batchDf.createOrReplaceTempView(customerTempView);

        val mergeQuery: String =
          s"""
            MERGE INTO $customerTable t
            USING $customerTempView s
            ON s.customer_id = t.customer_id
            WHEN MATCHED AND s.first_name != 'Unknown' THEN UPDATE SET
              t.first_name = s.first_name,
              t.last_name = s.last_name,
              t.gender = s.gender,
              t.date_of_birth = s.date_of_birth,
              t.email = s.email,
              t.phone_number = s.phone_number,
              t.country = s.country,
              t.customer_creation_date = s.customer_creation_date
            WHEN NOT MATCHED THEN INSERT (
              customer_id, first_name, last_name, gender,
              date_of_birth, email, phone_number, country,
              customer_creation_date
            ) VALUES (
              s.customer_id, s.first_name, s.last_name, s.gender,
              s.date_of_birth, s.email, s.phone_number, s.country,
              s.customer_creation_date
            )
          """;

        batchDf.sparkSession.sql(mergeQuery);
        println("============Upsert to silver_customer=============")
      };

    customerDf.writeStream
      .foreachBatch(batchProcess)
      .outputMode("update")
      .option(
        "checkpointLocation",
        s"s3a://${configVars.CHECKPOINT_BUCKET}/${configVars.CHECKPOINT_FOLDER}/silver_customer"
      )
      .start();
  }

  /** Upsert into silver_product (MERGE by product_id) */
  def upsertSilverProduct(
      productDf: DataFrame,
      productTable: String
  ): Unit = {
    val productTempView: String = "productSilverStg";
    val batchProcess: (DataFrame, Long) => Unit =
      (batchDf: DataFrame, batchId: Long) => {
        batchDf.createOrReplaceTempView(productTempView);

        val mergeQuery: String =
          s"""
            MERGE INTO $productTable t
            USING $productTempView s
            ON s.product_id = t.product_id
            WHEN MATCHED THEN UPDATE SET
              t.product_name = s.product_name,
              t.product_link = s.product_link,
              t.price = s.price,
              t.base_price = s.base_price,
              t.currency = s.currency,
              t.sale_percents = s.sale_percents,
              t.product_type = s.product_type
            WHEN NOT MATCHED THEN INSERT (
              product_id, product_name, product_link, price,
              base_price, currency, sale_percents, product_type
            ) VALUES (
              s.product_id, s.product_name, s.product_link, s.price,
              s.base_price, s.currency, s.sale_percents, s.product_type
            )
          """;

        batchDf.sparkSession.sql(mergeQuery);
        println("=========Upsert to silver_product============")
      };

    productDf.writeStream
      .foreachBatch(batchProcess)
      .outputMode("update")
      .option(
        "checkpointLocation",
        s"s3a://${configVars.CHECKPOINT_BUCKET}/${configVars.CHECKPOINT_FOLDER}/silver_product"
      )
      .start();
  }

  /** Insert into silver_transaction */
  def insertTransaction(df: DataFrame, table: String): Unit = {
    var tempView: String = "transactionStg";
    val batchProcess: (DataFrame, Long) => Unit =
      (batchDf: DataFrame, batchId: Long) => {

        val insertQuery =
          s"""INSERT INTO ${table}
            |SELECT
            |transaction_id,
            |customer_id,
            |product_id,
            |product_name,
            |price,
            |quantity,
            |total_amount,
            |event_time
            |FROM ${tempView}
            |""".stripMargin

        batchDf.createOrReplaceTempView(tempView)
        batchDf.sparkSession.sql(insertQuery);
        println("=========INSERT TO TRANSACTION============")

      };

    df.writeStream
      .foreachBatch(batchProcess)
      .outputMode("update")
      .option(
        "checkpointLocation",
        s"s3a://${configVars.CHECKPOINT_BUCKET}/${configVars.CHECKPOINT_FOLDER}/transaction"
      )
      .start();

  }

  // ── GOLD LAYER (SCD2 Merge) ───────────────────────────────────────────────

  /** SCD2 merge into gold.dim_customer:
    *   - Expire existing current records that match incoming customer_id
    *   - Insert new version with is_current=true
    */
  def mergeGoldDimCustomer(
      customerDf: DataFrame,
      dimCustomerTable: String
  ): Unit = {
    val customerTempView: String = "customerGoldStg";
    val batchProcess: (DataFrame, Long) => Unit =
      (batchDf: DataFrame, batchId: Long) => {
        val mergeQuery: String =
          s"""
            -- Step 1: Expire old current records
            UPDATE $dimCustomerTable t
            SET t.is_current = false, t.expired_date = current_date()
            WHERE t.is_current = true
            AND EXISTS (
                SELECT 1 FROM $customerTempView s
                WHERE s.customer_id = t.customer_id
            )
            """;

        val columns =
          """customer_sk, customer_id, first_name, last_name, gender,
                  date_of_birth, email, phone_number, country, customer_creation_date,
                  effective_date, expired_date, is_current""".stripMargin;

        val insertQuery: String =
          s"""
            -- Step 2: Insert new version
            INSERT INTO $dimCustomerTable (${columns})
            SELECT
                customer_sk,
                customer_id,
                first_name,
                last_name,
                gender,
                date_of_birth,
                email,
                phone_number,
                country,
                customer_creation_date,
                current_date() AS effective_date,
                CAST('9999-12-31' AS DATE) AS expired_date,
                true AS is_current
            FROM $customerTempView
          """;

        batchDf.createOrReplaceTempView(customerTempView);
        batchDf.sparkSession.sql(mergeQuery);
        batchDf.sparkSession.sql(insertQuery);
        println("============Merge to gold dim_customer=============")
      };

    customerDf.writeStream
      .foreachBatch(batchProcess)
      .outputMode("update")
      .option(
        "checkpointLocation",
        s"s3a://${configVars.CHECKPOINT_BUCKET}/${configVars.CHECKPOINT_FOLDER}/gold_dim_customer"
      )
      .start();
  }

  /** SCD2 merge into gold.dim_product:
    *   - Expire existing current records that match incoming product_id
    *   - Insert new version with is_current=true
    */
  def mergeGoldDimProduct(
      productDf: DataFrame,
      dimProductTable: String
  ): Unit = {
    val productTempView: String = "productGoldStg";
    val batchProcess: (DataFrame, Long) => Unit =
      (batchDf: DataFrame, batchId: Long) => {
        val mergeQuery: String =
          s"""
            -- Step 1: Expire old current records
            UPDATE $dimProductTable t
            SET t.is_current = false, t.expired_date = current_date()
            WHERE t.is_current = true
            AND EXISTS (
                SELECT 1 FROM $productTempView s
                WHERE s.product_id = t.product_id
            )
            """;

        val insertQuery: String =
          s"""
            -- Step 2: Insert new version
            INSERT INTO $dimProductTable
            SELECT
                product_sk,
                product_id,
                product_name,
                product_link,
                price,
                base_price,
                currency,
                sale_percents,
                product_type,
                current_date() AS effective_date,
                CAST('9999-12-31' AS DATE) AS expired_date,
                true AS is_current
            FROM $productTempView
          """;

        batchDf.createOrReplaceTempView(productTempView);
        batchDf.sparkSession.sql(mergeQuery);
        batchDf.sparkSession.sql(insertQuery);
        println("=========Merge to gold dim_product============")
      };

    productDf.writeStream
      .foreachBatch(batchProcess)
      .outputMode("update")
      .option(
        "checkpointLocation",
        s"s3a://${configVars.CHECKPOINT_BUCKET}/${configVars.CHECKPOINT_FOLDER}/gold_dim_product"
      )
      .start();
  }

  def writeFactTransaction(
      stgTransDf: DataFrame, // có is_new_customer flag
      dimCustomerTable: String,
      dimProductTable: String,
      dimDateTable: String,
      silverTransactionTable: String,
      hudiFactPath: String,
      hudiOptions: Map[String, String],
      checkpointLocation: String
  ): Unit = {

    stgTransDf.writeStream
      .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
        val spark = batchDf.sparkSession
        batchDf.cache()

        // ── Bước 1: Insert walk-in customers vào dim_customer ──────────
        // Đọc freshest dim_customer SAU KHI customer pipeline đã ghi
        val newCustomers = batchDf
          .filter(col("is_new_customer") === true)
          .select(
            md5(
              concat(
                col("customer_id"),
                current_date().cast(StringType)
              )
            ).alias("customer_sk"),
            col("customer_id"),
            col("phone_number"),
            lit("Unknown").alias("first_name"),
            lit("Unknown").alias("last_name"),
            lit("Unknown").alias("gender"),
            lit(null).cast(DateType).alias("date_of_birth"),
            lit("Unknown").alias("email"),
            lit("Unknown").alias("country"),
            current_date().alias("customer_creation_date")
          )

        if (!newCustomers.isEmpty) {
          newCustomers.createOrReplaceTempView("newCustStg")

          spark.sql(s"""
          UPDATE $dimCustomerTable
          SET is_current = false, expired_date = current_date()
          WHERE is_current = true
          AND customer_id IN (SELECT customer_id FROM newCustStg)
        """)

          spark.sql(s"""
          INSERT INTO $dimCustomerTable
          SELECT
            customer_sk, customer_id, first_name, last_name, gender,
            date_of_birth, email, phone_number, country, customer_creation_date,
            current_date()             AS effective_date,
            CAST('9999-12-31' AS DATE) AS expired_date,
            true                       AS is_current
          FROM newCustStg
        """)

          println(
            s"[Batch $batchId] Inserted ${newCustomers.count()} walk-in customers"
          )
        }

        // ── Bước 2: Đọc dim tables SAU KHI đã insert ──────────────────
        // spark.table() trong foreachBatch sẽ đọc state mới nhất
        val customerDim = spark
          .table(dimCustomerTable)
          .where(col("is_current") === true)
          .select(
            col("customer_sk"),
            col("customer_id").alias("c_customer_id")
          )

        val productDim = spark
          .table(dimProductTable)
          .where(col("is_current") === true)
          .select(
            col("product_sk"),
            col("product_id").alias("p_product_id")
          )

        val dateDim = spark
          .table(dimDateTable)
          .select(
            col("date_key"),
            col("date").alias("d_date")
          )

        // ── Bước 3: Join để resolve surrogate keys ─────────────────────
        val factBatch = batchDf
          .drop("is_new_customer")
          .join(
            broadcast(customerDim),
            col("customer_id") === col("c_customer_id"),
            "left"
          )
          .drop("c_customer_id")
          .join(
            broadcast(productDim),
            col("product_id") === col("p_product_id"),
            "left"
          )
          .drop("p_product_id")
          .join(
            broadcast(dateDim),
            col("timestamp").cast(DateType) === col("d_date"),
            "left"
          )
          .drop("d_date")
          .withColumn("transaction_sk", expr("uuid()"))
          .withColumn(
            "customer_sk",
            coalesce(col("customer_sk"), lit("Unknown"))
          )
          .withColumn("product_sk", coalesce(col("product_sk"), lit("Unknown")))
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
          )

        // ── Bước 4: Ghi vào Hudi fact_transaction ─────────────────────
        factBatch.write
          .format("hudi")
          .options(hudiOptions)
          .mode("append")
          .save(hudiFactPath)

        batchDf.unpersist()
        println(s"[Batch $batchId] fact_transaction written")
      }
      .outputMode("update")
      .option("checkpointLocation", checkpointLocation)
      .start()
  }
}
