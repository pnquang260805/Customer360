import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.IntegerType
import services.DeltaServices
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

    var conf = new SparkConf().setMaster("spark://master:7077").setAppName("pipeline");

    // Config S3
    conf.set("spark.sql.extension", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .set("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .set("fs.s3a.connection.establish.timeout", "15000")

    // Config Neo4j
    conf.set("neo4j.url", NEO4J_URL)
        .set("neo4j.authentication.basic.username", NEO4J_USERNAME)
        .set("neo4j.authentication.basic.password", NEO4J_PASSWORD)
        .set("neo4j.database", NEO4J_DBNAME);

    // config catalog
    conf.set("spark.sql.catalogImplementation", "hive")
        .set("spark.hadoop.hive.metastore.uris", "thrift://metastore:9083")
        .set("spark.hadoop.hive.metastore.warehouse.dir", "s3a://lake/")
    
    val spark = SparkSession.builder().config(conf).getOrCreate();

    spark.sparkContext.setLogLevel("WARN");
    import spark.implicits._
    
    var rawDb : String = "raw";
    var rawTable : String = "raw_table";

    var deltaService : DeltaServices = new DeltaServices(spark);
    deltaService.createDatabase(rawDb, "s3a://lake/raw/raw_db/");
    deltaService.createRawTable(rawDb, rawTable, "s3a://lake/raw/raw_table/");

  
//   var eventService : EventService = new EventService(spark, bootstrap = KAFKA_BOOTSTRAP)
//   var df = eventService.readStreamKafka(topic = KAFKA_TOPIC);
//   df.writeStream.format("console").outputMode("append").option("truncate", "false").start();

//   spark.streams.awaitAnyTermination();
}