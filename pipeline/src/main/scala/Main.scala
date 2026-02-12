import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.IntegerType
import services.HudiService
import org.apache.spark.sql.SaveMode
import services.EventService
// import org.apache.spark.sql.

object Main extends App {
    val S3_ACCESS_KEY : String = "admin";
    val S3_SECRET_KEY : String = "password";

    val KAFKA_BOOTSTRAP : String = "kafka:29092";
    val KAFKA_TOPIC : String = "event-topic";

    val NEO4J_URL : String = "bolt://neo4j:7687";
    val NEO4J_USERNAME : String = "neo4j";
    val NEO4J_PASSWORD : String = "capstoneptit";
    val NEO4J_DBNAME : String = "identity-graph";
    val BUCKET : String = "tables";

    var conf = new SparkConf().setMaster("spark://master:7077").setAppName("pipeline");

    // Config S3
    conf.set("spark.sql.extensions","org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .set("'spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .set("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .set("fs.s3a.connection.establish.timeout", "15000")
        .set("spark.kryo.registrator","org.apache.spark.HoodieSparkKryoRegistrar")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // Config Neo4j
    conf.set("neo4j.url", NEO4J_URL)
        .set("neo4j.authentication.basic.username", NEO4J_USERNAME)
        .set("neo4j.authentication.basic.password", NEO4J_PASSWORD)
        .set("neo4j.database", NEO4J_DBNAME);
    
    val spark = SparkSession.builder().config(conf).getOrCreate();

    spark.sparkContext.setLogLevel("WARN");
    import spark.implicits._
    
    var rawDb : String = "raw";
    var rawTable : String = "raw_table";
    var catalogName : String = "hudi"

    var hudiService : HudiService = new HudiService(spark);
    hudiService.createDatabase(rawDb, s"s3a://$BUCKET/bronze/raw_db/");
    hudiService.createRawTable(rawDb, rawTable, s"s3a://$BUCKET/bronze/raw_table/");

  
    var eventService : EventService = new EventService(spark, bootstrap = KAFKA_BOOTSTRAP)
    var df = eventService.readStreamKafka(topic = KAFKA_TOPIC);
    
    hudiService.writeStreamTable(df, s"s3a://$BUCKET/checkpoint/", rawDb, rawTable);

    spark.streams.awaitAnyTermination();
}