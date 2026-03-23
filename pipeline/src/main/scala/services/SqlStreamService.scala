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
                val mergeQuery : String = s"""
                    -- Step 1: Merge and set
                    MERGE INTO $customerTable AS target
                    USING $customerTempView AS source
                    ON target.customer_id = source.customer_id AND target.is_current = true
                    WHEN MATCHED THEN
                        UPDATE SET 
                            target.expired_date = current_date(),
                            target.is_current = false,
                            target.creation_date = source.creation_date
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
}
