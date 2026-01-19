import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Main extends App {
  val S3_ACCESS_KEY : String = "admin";
  val S3_SECRET_KEY : String = "password";

  var conf = new SparkConf().setMaster("spark://master:7077")
  var spark = SparkSession.builder().appName("pipeline")
                                    .config(conf).config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
                                    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
                                    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
                                    .config("spark.hadoop.fs.s3a.path.style.access", "true")
                                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
                                    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") 
                                    .getOrCreate()
  var df : DataFrame = spark.read.format("json").option("pathGlobFilter", "*.jsonl").load("s3a://files/mongodb/customers");
  var exploded_df = df.select(col("_airbyte_data.customer_id").as("customer_id"),
                              col("_airbyte_data.first_name").as("first_name"),
                              col("_airbyte_data.last_name").as("last_name"),
                              col("_airbyte_data.gender").as("gender"),
                              col("_airbyte_data.dob").as("date_of_birth"),
                              col("_airbyte_data.phone_number").as("phone_number"),
                              col("_airbyte_data.email").as("email"),
                              col("_airbyte_data.address").as("address"),
                              col("_airbyte_data.country").as("country"));
  exploded_df.show(truncate=false);
}