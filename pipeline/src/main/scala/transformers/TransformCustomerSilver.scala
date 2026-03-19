package transformers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.functions.{col, from_json, when, lit}
import org.apache.spark.sql.types._
import interfaces.Transform

class TransformCustomerSilver extends Transform{
    def transformFromRaw(rawDf: DataFrame): DataFrame = {
        val dataSchema = StructType(Seq(
            StructField("customer_id", StringType, true),
            StructField("first_name", StringType, true),
            StructField("last_name", StringType, true),
            StructField("gender", StringType, true),
            StructField("date_of_birth", IntegerType, true), // Debezium gửi ngày dạng số nguyên (Epoch days)
            StructField("email", StringType, true),
            StructField("phone_number", StringType, true),
            StructField("address", StringType, true),
            StructField("country", StringType, true),
            StructField("creation_date", IntegerType, true)
        ));

        val cdcSchema = StructType(Seq(
            // StructField("before", dataSchema, true), 
            StructField("after", dataSchema, true),
            // StructField("op", StringType, true),     // 'c' cho create, 'u' cho update, 'd' cho delete
            // StructField("ts_ms", LongType, true)
        ));

        var rawCustomer : DataFrame = rawDf.where(col("key") === "customer").select("value");
        var customerDf : DataFrame = rawCustomer.select(from_json(col("value").cast(StringType), cdcSchema).alias("after_value"))
        var explodedDf : DataFrame = customerDf.select(col("after_value.*"))

        var droppedCustomer : DataFrame = explodedDf.na.drop(how="any", cols = Seq("customer_id")).dropDuplicates();

        return null;
    }
    
    def transformFromSilver(silverDf: DataFrame): DataFrame = {
        return null;
    }
    
}
