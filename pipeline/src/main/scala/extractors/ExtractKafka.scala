package extracts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

class ExtractKafka(spark : SparkSession, bootstrap : String){
    def extractStreamKafka(topic: String, startOffSet: String = "earliest", options: Map[String, String] = Map.empty): DataFrame = {
        var baseOptions = Map(
            "kafka.bootstrap.servers" -> bootstrap,
            "subscribe" -> topic,
            "startingOffsets" -> startOffSet
        );
        var fullOptions = baseOptions ++ options;
        var eventDf = spark.readStream.format("kafka")
                    .options(fullOptions) // options có "s"
                    .load()
                    .selectExpr("CAST (offset AS STRING)", "topic", "CAST (key AS STRING)","CAST (value AS STRING)"); 
        return eventDf;
    }
}