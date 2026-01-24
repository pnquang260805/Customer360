import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.IntegerType
import services.Ingestion
import services.DeltaServices

object Main extends App {
  val S3_ACCESS_KEY : String = "admin";
  val S3_SECRET_KEY : String = "password";

  val KAFKA_BOOTSTRAP : String = "kafka:29092";
  val KAFKA_TOPIC : String = "test-topic";

  var conf = new SparkConf().setMaster("spark://master:7077").setAppName("pipeline");
  conf.set("spark.sql.extension", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .set("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
      .set("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
      .set("spark.hadoop.fs.s3a.path.style.access", "true")
      .set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
      .set("fs.s3a.connection.establish.timeout", "15000")
  val spark = SparkSession.builder().config(conf).getOrCreate();

  spark.sparkContext.setLogLevel("WARN")
  // var raw_df = spark.readStream.format("kafka")
  //                           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
  //                           .option("subscribe", KAFKA_TOPIC)
  //                           .option("startingOffsets", "earliest")
  //                           .load();
  var delta = new DeltaServices(spark);
  delta.createDatabase("raw", "s3a://bronze/raw_db");
  delta.createRawTable("raw", "raw_table", "s3a://bronze/raw_table")
  var df = spark.read.table("raw.raw_table");
  df.show();
}