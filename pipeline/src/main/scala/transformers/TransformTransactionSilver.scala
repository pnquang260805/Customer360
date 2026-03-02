package transformers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.functions.{col, from_json, when, lit}
import org.apache.spark.sql.types.{StringType, IntegerType, DoubleType, DecimalType}

class TransformTransactionSilver {
    def transform(df : DataFrame): DataFrame = {
        var transactionDf = df.filter(col("key") === "transaction");

        val schema : StructType = StructType(Seq(
            StructField("transaction_id", StringType, false),
            StructField("customer_id", StringType, true),
            StructField("product_id", StringType, true),
            StructField("product_name", StringType, true),
            StructField("price", DecimalType(10, 2), true),
            StructField("quantity", DecimalType(10, 2), true),
            StructField("total_amount", DecimalType(10, 2), true),
            StructField("event_time", StringType, true),
        ));

        var extractedDf : DataFrame = transactionDf.withColumn("json_data", from_json(col("value"), schema)).select(col("json_data.*"));
        
        var parseNullDf : DataFrame = extractedDf
            .withColumn("customer_id", 
                when(col("customer_id") === "Null", lit(null).cast(StringType))
                .otherwise(col("customer_id")).cast(StringType))
            .withColumn("product_id", 
                when(col("product_id") === "Null", lit(null).cast(StringType))
                .otherwise(col("product_id")).cast(StringType))
            .withColumn("product_name", 
                when(col("product_name") === "Null", lit(null).cast(StringType))
                .otherwise(col("product_name")).cast(StringType))
            .withColumn("event_time", 
                when(col("event_time") === "Null", lit(null).cast(StringType))
                .otherwise(col("event_time")).cast(StringType))
            .withColumn("price", col("price").cast(IntegerType))
            .withColumn("quantity", col("quantity").cast(IntegerType))
        // Temp
        var droppedNullDf = parseNullDf.na.drop(Seq("transaction_id", "customer_id", "product_id", "event_time"));
        return droppedNullDf;
    }
}
