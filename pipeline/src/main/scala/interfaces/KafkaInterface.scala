package interfaces

import org.apache.spark.sql.DataFrame

trait KafkaInterface {
  def readStreamKafka(topic : String, startOffSet : String = "earliest", options : Map[String, String] = Map()): DataFrame;
}
