package services

import interfaces.KafkaInterface
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

class EventService(spark : SparkSession, bootstrap : String) extends KafkaInterface{
    def readStreamKafka(topic: String, startOffSet: String = "earliest", options: Map[String, String] = Map.empty): DataFrame = {
        var baseOptions = Map(
            "kafka.bootstrap.servers" -> bootstrap,
            "subscribe" -> topic,
            "startingOffsets" -> startOffSet
        );
        var fullOptions = baseOptions ++ options;
        var eventDf = spark.readStream.format("kafka").options(fullOptions).load().selectExpr("key","CAST (key AS STRING)","CAST (value AS STRING)"); // options có "s"
        return eventDf;
    }
}