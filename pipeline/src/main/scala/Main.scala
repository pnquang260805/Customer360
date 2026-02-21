import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.IntegerType
import services.HudiService
import org.apache.spark.sql.SaveMode
import extractors.ExtractKafka
import loaders.LoadHudi
import config.SparkConfig
import extracts.ExtractKafka

object Main extends App {
    // Base variables
    val S3_ACCESS_KEY : String = "admin";
    val S3_SECRET_KEY : String = "password";

    val KAFKA_BOOTSTRAP : String = "kafka:29092";

    val NEO4J_URL : String = "bolt://neo4j:7687";
    val NEO4J_USERNAME : String = "neo4j";
    val NEO4J_PASSWORD : String = "capstoneptit";
    val NEO4J_DBNAME : String = "identity-graph";
    val BUCKET : String = "tables";

    val RAW_2_BRONZE_TOPIC : String = "raw-2-bronze-topic";
    val BRONZE_2_SILVER_TOPIC :  String = "bronze-2-silver";
    val SILVER_2_GOLD_TOPIC : String = "silver-2-gold";

    //region Spark
    var sparkConf = new SparkConfig("spark://master:7077")
    sparkConf.configS3(S3_ACCESS_KEY, S3_SECRET_KEY)
    sparkConf.configNeo4j(NEO4J_URL, NEO4J_USERNAME, NEO4J_PASSWORD, NEO4J_DBNAME)
    
    val spark = SparkSession.builder().config(sparkConf.getConf()).getOrCreate();

    spark.sparkContext.setLogLevel("WARN");
    import spark.implicits._
    //endregion

    // region Initial dependencies
    var hudiService : HudiService = new HudiService(spark);
    var kafkaExtractor: ExtractKafka = new ExtractKafka(spark, KAFKA_BOOTSTRAP);
    var hudiLoader : LoadHudi = new LoadHudi(spark)
    // endregion

    var rawDb : String = "raw";
    var rawTable : String = "raw_table";
    var catalogName : String = "hudi"

    // Initiate database
    hudiService.createDatabase(rawDb, s"s3a://$BUCKET/bronze/raw_db/");
    hudiService.createRawTable(rawDb, rawTable, s"s3a://$BUCKET/bronze/raw_table/");

    // Extract
    var rawStreamDf = kafkaExtractor.extractStreamKafka(topic = RAW_2_BRONZE_TOPIC);    
    
    // Load
    // Load data into raw
    hudiLoader.loadStreamTable(rawStreamDf, s"s3a://$BUCKET/checkpoint/", rawDb, rawTable);

    // Transform


    spark.streams.awaitAnyTermination();
}