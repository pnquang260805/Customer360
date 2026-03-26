package services

import org.apache.spark.sql.DataFrame
import java.time.LocalDate
import config.ConfigVariables
import java.util.UUID

class SqlStreamService {
    val configVars = new ConfigVariables();

    private def createUpdateColumns(cols : Seq[String]): String = {
        cols.map(c => s"$c = source.$c").mkString(",");
    }

    private def getNow(): String = {
        var now:LocalDate = LocalDate.now();
        return now.toString();
    }

    private def genSk(): String = {
        return UUID.randomUUID().toString();
    }

    def mergeCustomerSilver(customerDf : DataFrame, customerTable: String): Unit = {
        var customerTempView : String = "customerStg";
        // var columns: Seq[String] = Seq("first_name", "last_name", "gender", "date_of_birth", "email", "phone_number", "country", "creation_date");
        // val columnListString = columns.mkString(", ");
        val batchProcess : (DataFrame, Long) => Unit = (batchDf : DataFrame, batchId : Long) => {
            if(!batchDf.isEmpty){
                val customerIds = batchDf.select("customer_id").distinct().collect().map(r => s"'${r.getString(0)}'").mkString(",")
                val mergeQuery : String = s"""
                    -- Step 1: Merge and set
                    UPDATE $customerTable 
                    SET is_current = false, expired_date = current_date()
                    WHERE customer_id IN ($customerIds) AND is_current = true
                    """;

                val insertQuery: String = s"""
                    INSERT INTO $customerTable
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
            }
        };
        customerDf.writeStream
                .foreachBatch(batchProcess)
                .outputMode("update")
                .option("checkpointLocation", s"s3a://${configVars.BUCKET}/checkpoint/customer_silver")
                .start();
    }
    
    def mergeProductDim(productDf : DataFrame, productTable: String): Unit = {
        var productTempView : String = "productStg";
        
        val batchProcess : (DataFrame, Long) => Unit = (batchDf : DataFrame, batchId : Long) => {
            if(!batchDf.isEmpty){
                
                val productIds = batchDf.select("product_id").distinct().collect().map(r => s"'${r.getString(0)}'").mkString(",")
                
                val mergeQuery : String = s"""
                    -- Step 1: Merge and set
                    UPDATE $productTable 
                    SET is_current = false, expired_date = current_date()
                    WHERE product_id IN ($productIds) AND is_current = true
                    """;

                val insertQuery: String = s"""
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
            }
        };
        
        productDf.writeStream
                .foreachBatch(batchProcess)
                .outputMode("update")
                .option("checkpointLocation", s"s3a://${configVars.BUCKET}/checkpoint/dim_product") 
                .start();
    }

    def mergeTransactionSilver(transactionDf : DataFrame, transactionTable: String): Unit = {
        var transactionTempView : String = "transactionStg";
        
        val batchProcess : (DataFrame, Long) => Unit = (batchDf : DataFrame, batchId : Long) => {
            if(!batchDf.isEmpty){
                // Bảng Fact không cần SCD Type 2, chỉ cần Upsert thẳng bằng MERGE INTO của Hudi
                val mergeQuery : String = s"""
                    MERGE INTO $transactionTable AS target
                    USING $transactionTempView AS source
                    ON target.transaction_id = source.transaction_id
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """;

                batchDf.createOrReplaceTempView(transactionTempView);
                batchDf.sparkSession.sql(mergeQuery);
            }
        };
        
        transactionDf.writeStream
                .foreachBatch(batchProcess)
                .outputMode("update")
                .option("checkpointLocation", s"s3a://${configVars.BUCKET}/checkpoints/silver_transaction") 
                .start();
    }
}
