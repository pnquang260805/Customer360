import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.IntegerType
import services.HudiService
import org.apache.spark.sql.SaveMode
import extractors.ExtractKafka
import config.SparkConfig
import extractors.ExtractKafka
import config.{ConfigVariables, DatalakeConfig}
import transformers.TransformCustomerSilver
import services.SqlStreamService
import transformers.TransformProductSilver
import services.MongoDbService
import transformers.TransformEvent
import utils.MUtils

object Main extends App with LazyLogging with MUtils {
  // Base variables
  val configVars = new ConfigVariables()
  val datalakeConf = new DatalakeConfig()

  // Spark
  var sparkConf = new SparkConfig("spark://master:7077")
  sparkConf.configS3(configVars.S3_ACCESS_KEY, configVars.S3_SECRET_KEY)

  val spark = SparkSession.builder().config(sparkConf.getConf()).enableHiveSupport().getOrCreate();

  spark.sparkContext.setLogLevel("WARN");

  import spark.implicits._

  //Initial dependencies
  var hudiService: HudiService = new HudiService(spark);
  var kafkaExtractor: ExtractKafka = new ExtractKafka(spark, configVars.KAFKA_BOOTSTRAP);
  var transformCustomerSilver: TransformCustomerSilver = new TransformCustomerSilver();
  var sqlService: SqlStreamService = new SqlStreamService();
  var transformProduct: TransformProductSilver = new TransformProductSilver();
  var mongodbService: MongoDbService = new MongoDbService(spark);
  var transformEvent: TransformEvent = new TransformEvent();
  // var transformTransactionSilver : TransformTransactionSilver = new TransformTransactionSilver();

  // Initiate database
  hudiService.createDatabase(datalakeConf.rawDb, s"s3a://${configVars.BUCKET}/bronze/raw_db/");
  hudiService.createRawTable(datalakeConf.rawDb, datalakeConf.rawTable, s"s3a://${configVars.BUCKET}/bronze/raw_table/");

  hudiService.createDatabase(datalakeConf.silverDb, s"s3a://${configVars.BUCKET}/silver/silver_db/");
  hudiService.createSilverTransaction(datalakeConf.silverDb, datalakeConf.silverTransactionTable, s"s3a://${configVars.BUCKET}/silver/silver_transaction/");
  hudiService.createSilverCustomer(datalakeConf.silverDb, datalakeConf.silverCustomerTable, s"s3a://${configVars.BUCKET}/silver/silver_customer");
  hudiService.createDimProduct(datalakeConf.silverDb, datalakeConf.dimProduct, s"s3a://${configVars.BUCKET}/silver/dim_product");

  // Extract
  var customerStreamDf = kafkaExtractor.extractStreamKafka(topic = configVars.RAW_CUSTOMER_TOPIC);
  var productStreamDf = kafkaExtractor.extractStreamKafka(configVars.PRODUCT_TOPIC);
  var eventStreamDf = kafkaExtractor.extractStreamKafka(configVars.EVENT_TOPIC);
  // Load
  // Load data into raw
  val combinedRawDf = customerStreamDf
    .union(productStreamDf)
    .union(eventStreamDf)

  benchmark("Pipeline") {
    hudiService.writeRaw(combinedRawDf, "all_topics_raw")

    // Transform
    //    var rawDf = hudiService.readStreamTable(s"s3a://${configVars.BUCKET}/bronze/raw_table/"); // khả năng nghẽn chỗ này
    var rawDf = combinedRawDf;
    // rawDf.writeStream.format("console").start(); // for debug

    var stgCustomerSilver: DataFrame = transformCustomerSilver.stgSilver(rawDf);
    sqlService.mergeCustomerDim(stgCustomerSilver, s"${datalakeConf.silverDb}.${datalakeConf.silverCustomerTable}")
    mongodbService.writeMongoDb(stgCustomerSilver,
      configVars.ATLAS_DATABASE,
      configVars.ATLAS_COLLECTION,
      configVars.atlasConnectionString(),
      "customer_id",
      configVars.CHECKPOINT_MONGODB_CUSTOMER);
    // stgCustomerSilver.writeStream.format("console").start();

    // Product
    var productDf: DataFrame = rawDf.filter(col("key") === "product");
    var stgDimProduct: DataFrame = transformProduct.stgSilver(productDf);
    sqlService.mergeProductDim(stgDimProduct, s"${datalakeConf.silverDb}.${datalakeConf.dimProduct}");

    // Event
    var stgEvent: DataFrame = transformEvent.transformStgEvent(rawDf);
    mongodbService.writeMongoDb(stgEvent,
      configVars.ATLAS_DATABASE,
      configVars.ATLAS_COLLECTION,
      configVars.atlasConnectionString(),
      "customer_id",
      configVars.CHECKPOINT_MONGODB_EVENT);
    logger.info("Inserted to mongodb")
  }

  spark.streams.awaitAnyTermination();
}