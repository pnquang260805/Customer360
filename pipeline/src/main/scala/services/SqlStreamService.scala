package services
import org.apache.spark.sql.DataFrame

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

  def mergeCustomerDim(customerDf: DataFrame, customerTable: String): Unit = {
    var customerTempView: String = s"customerStg";
    val batchProcess: (DataFrame, Long) => Unit =
      (batchDf: DataFrame, batchId: Long) => {
        // val customerIds = batchDf.select("customer_id").distinct().collect().map(r => s"'${r.getString(0)}'").mkString(",")
        val mergeQuery: String =
          s"""
                    -- Step 1: Merge and set
                    UPDATE $customerTable t
                    SET t.is_current = false, t.expired_date = current_date()
                    WHERE t.is_current = true
                    AND EXISTS (
                        SELECT 1 FROM $customerTempView s
                        WHERE trim(s.customer_id) = trim(t.customer_id)
                    )
                    """;
        val columns =
          """customer_sk, customer_id, first_name, last_name, gender,
                  date_of_birth, email, phone_number, country, creation_date, effective_date, expired_date, is_current""".stripMargin;
        val insertQuery: String =
          s"""
                    INSERT INTO $customerTable (${columns})
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
                        creation_date,
                        current_date() AS effective_date,
                        CAST('9999-12-31' AS DATE) AS expired_date,
                        true AS is_current
                    FROM $customerTempView
                """;

        batchDf.createOrReplaceTempView(customerTempView);
        batchDf.sparkSession.sql(mergeQuery);
        batchDf.sparkSession.sql(insertQuery);
        print("============Merge to customer=============")

      };
    customerDf.writeStream
      .foreachBatch(batchProcess)
      .outputMode("update")
      .option(
        "checkpointLocation",
        s"s3a://${configVars.CHECKPOINT_BUCKET}/${configVars.CHECKPOINT_FOLDER}/customer_silver"
      )
      .start();
  }

  def mergeProductDim(productDf: DataFrame, productTable: String): Unit = {
    var productTempView: String = "productStg";
    val batchProcess: (DataFrame, Long) => Unit =
      (batchDf: DataFrame, batchId: Long) => {

        val mergeQuery: String =
          s"""
                    -- Step 1: Merge and set
                    UPDATE $productTable t
                    SET t.is_current = false, t.expired_date = current_date()
                    WHERE t.is_current = true
                    AND EXISTS (
                        SELECT 1 FROM $productTempView s
                        WHERE trim(s.product_id) = trim(t.product_id)
                    )
                    """;

        val insertQuery: String =
          s"""
                    INSERT INTO $productTable
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
        print("=========Merge to product============")

      };

    productDf.writeStream
      .foreachBatch(batchProcess)
      .outputMode("update")
      .option(
        "checkpointLocation",
        s"s3a://${configVars.CHECKPOINT_BUCKET}/${configVars.CHECKPOINT_FOLDER}/dim_product"
      )
      .start();

  }

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
        print("=========INSERT TO TRANSACTION============")

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
}
