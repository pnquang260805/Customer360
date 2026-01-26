import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.IntegerType
import services.Ingestion
import services.DeltaServices
import org.apache.spark.sql.SaveMode
// import org.apache.spark.sql.

object Main extends App {
  val S3_ACCESS_KEY : String = "admin";
  val S3_SECRET_KEY : String = "password";

  val KAFKA_BOOTSTRAP : String = "kafka:29092";
  val KAFKA_TOPIC : String = "test-topic";

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
  
  val spark = SparkSession.builder().config(conf).getOrCreate();

  spark.sparkContext.setLogLevel("WARN");
  import spark.implicits._
  // var raw_df = spark.readStream.format("kafka")
  //                           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
  //                           .option("subscribe", KAFKA_TOPIC)
  //                           .option("startingOffsets", "earliest")
  //                           .load();
  // var delta = new DeltaServices(spark);
  // delta.createDatabase("raw", "s3a://bronze/raw_db");
  // delta.createRawTable("raw", "raw_table", "s3a://bronze/raw_table")
  // var df = spark.read.table("raw.raw_table");
  // df.show();

  var df1 = List(
      (1, "John", "Doe", 42),
      (2, "Jane", "Doe", 40)
  ).toDF("id", "name", "surname", "age");
  df1.show();

  var product = List(
    (1, "ABC"),
    (2, "DEF"),
    (3, "1234")
  ).toDF("id", "name")

  var transaction = List(
    (1, 1),
    (2, 1),
    (1, 2),
    (3, 2),
  ).toDF("product_id", "customer_id");

  var df = df1.as("customer").join(transaction.as("t"), $"customer.id" === $"t.customer_id")  // 3 dấu =
                  .join(product.as("p"), $"p.id" === $"t.product_id");
  
  df = df.select(
    col("customer.id").as("customer_id"),
    col("customer.name").as("customer_name"),
    col("p.id").as("product_id"),
    col("p.name").as("product_name")
  );
  df.show();

  df.write.format("org.neo4j.spark.DataSource")
          .mode(SaveMode.Append)
          .option("relationship", "BUY")
          .option("relationship.save.strategy", "keys")

          .option("relationship.source.save.mode", "Overwrite") // Để overwrite để tránh tạo ra 8 node
          .option("relationship.source.labels", ":Person")
          .option("relationship.source.node.keys", "customer_id:id") // customer_id:id -> lưu customer_id vào trường id của neo4j
          .option("relationship.source.node.properties", "customer_name:name") // customer_name:name -> lưu customer_name vào property name của node Person

          .option("relationship.target.save.mode", "Overwrite")
          .option("relationship.target.labels", ":Product") // có dấu :
          .option("relationship.target.node.keys", "product_id:id") 
          .option("relationship.target.node.properties", "product_name:name")
          .save();
}