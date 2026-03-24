package services

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.HashMap
import org.apache.spark.sql.SaveMode

class Neo4jService(spark : SparkSession) {

    private val NEO4J_URI : String = System.getenv("NEO4J_URI");
    private val NEO4J_USERNAME : String = System.getenv("NEO4J_USERNAME");
    private val NEO4J_PASSWORD : String = System.getenv("NEO4J_PASSWORD");
    private val NEO4J_DATABASE : String = System.getenv("NEO4J_DATABASE");
    private val baseOption : HashMap[String, String] = HashMap(
            "url" -> NEO4J_URI,
            "authentication.basic.username" -> NEO4J_USERNAME,
            "authentication.basic.password" -> NEO4J_PASSWORD,
        );
    def queryDatabase(query: String, conf : HashMap[String, String] = new HashMap()): DataFrame = {
        var opts = baseOption + ("query" -> query); 
        var finalOptions = opts ++ conf;

        var df : DataFrame = spark.read.format("org.neo4j.spark.DataSource")
                                    .options(finalOptions)
                                    .load();
        return df;
    }

    def writeDf(df: DataFrame, query: String): Unit = {
        var opts = baseOption + ("query" -> query); 
        df.writeStream.format("org.neo4j.spark.DataSource")
                .outputMode("append")
                .options(opts)
                .option("checkpointLocation", "s3a://lake/checkpoints/neo4j_customer")
                .start();
    }
}
