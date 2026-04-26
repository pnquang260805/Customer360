package transformers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, TimestampType}
import org.apache.spark.sql.functions._

class TransformEvent {
    def transformStgEvent(rawDf : DataFrame) : DataFrame = {
        val dataSchema : StructType = StructType(Seq(
            StructField("event_id", StringType, false),
            StructField("type", StringType, false),
            StructField("customer_id", StringType, false),
            StructField("url", StringType, false),
            StructField("time_stamp", TimestampType, false),
        ));
        val cdcSchema = StructType(Seq(
            // StructField("before", dataSchema, true), 
            StructField("after", dataSchema, true),
            // StructField("op", StringType, true),     // 'c' cho create, 'u' cho update, 'd' cho delete
            // StructField("ts_ms", LongType, true)
        ));
        var rawEvent : DataFrame = rawDf.where(col("key") === "event").select("value");
        var eventDf : DataFrame = rawEvent.select(from_json(col("value").cast(StringType), cdcSchema).alias("after_value"))
        var explodedDf : DataFrame = eventDf.select(col("after_value.after.*"))
        
        var finalDf : DataFrame = explodedDf.withWatermark("time_stamp", "5 minutes") // stream aggregate cần watermark
                                            .groupBy("customer_id").agg(collect_list(
                                                struct("event_id", "type", "url", "time_stamp")
                                            ).alias("activity_log"))

        return finalDf;
    }
}
