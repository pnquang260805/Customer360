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
import config.{ConfigVariables, DatalakeConfig}
import transformers.{
  TransformCustomerSilver,
  TransformDimCustomer,
  TransformDimDate,
  TransformDimProduct,
  TransformEvent,
  TransformFactTransaction,
  TransformProductSilver,
  TransformTransactionSilver,
  TransformUnifiedProfile
}
import services.SqlStreamService
import services.MongoDbService

object Main extends App {
  // Base variables
  val configVars = new ConfigVariables()
  val datalakeConf = new DatalakeConfig()

  // Spark
  var sparkConf = new SparkConfig("spark://master:7077")
  sparkConf.configS3(configVars.S3_ACCESS_KEY, configVars.S3_SECRET_KEY)

  val spark = SparkSession
    .builder()
    .config(sparkConf.getConf())
    .enableHiveSupport()
    .getOrCreate();

  spark.sparkContext.setLogLevel("WARN");

  import spark.implicits._

  // ══════════════════════════════════════════════════════════════════════════
  // Initial dependencies
  // ══════════════════════════════════════════════════════════════════════════
  var hudiService: HudiService = new HudiService(spark);
  var kafkaExtractor: ExtractKafka =
    new ExtractKafka(spark, configVars.KAFKA_BOOTSTRAP);
  var transformCustomerSilver: TransformCustomerSilver =
    new TransformCustomerSilver();
  var sqlService: SqlStreamService = new SqlStreamService();
  var transformProduct: TransformProductSilver = new TransformProductSilver();
  var mongodbService: MongoDbService = new MongoDbService(spark);
  var transformEvent: TransformEvent = new TransformEvent();
  var transformTransactionSilver: TransformTransactionSilver =
    new TransformTransactionSilver(hudiService, mongodbService);
  var transformDimCustomer: TransformDimCustomer =
    new TransformDimCustomer(spark);
  var transformDimProduct: TransformDimProduct =
    new TransformDimProduct(spark);
  var transformDimDate: TransformDimDate =
    new TransformDimDate(spark);
  var transformFactTransaction: TransformFactTransaction =
    new TransformFactTransaction(spark);
  var transformUnifiedProfile: TransformUnifiedProfile =
    new TransformUnifiedProfile(spark);

  // ══════════════════════════════════════════════════════════════════════════
  // Initiate databases & tables
  // ══════════════════════════════════════════════════════════════════════════

  // -- RAW LAYER --
  hudiService.createDatabase(
    datalakeConf.rawDb,
    s"s3a://${configVars.BUCKET}/bronze/raw_db/"
  );
  hudiService.createRawTable(
    datalakeConf.rawDb,
    datalakeConf.rawTable,
    s"s3a://${configVars.BUCKET}/bronze/raw_table/"
  );

  // -- SILVER LAYER --
  hudiService.createDatabase(
    datalakeConf.silverDb,
    s"s3a://${configVars.BUCKET}/silver/silver_db/"
  );
  hudiService.createSilverCustomer(
    datalakeConf.silverDb,
    datalakeConf.silverCustomerTable,
    s"s3a://${configVars.BUCKET}/silver/silver_customer"
  );
  hudiService.createSilverProduct(
    datalakeConf.silverDb,
    datalakeConf.silverProductTable,
    s"s3a://${configVars.BUCKET}/silver/silver_product"
  );
  hudiService.createSilverTransaction(
    datalakeConf.silverDb,
    datalakeConf.silverTransactionTable,
    s"s3a://${configVars.BUCKET}/silver/silver_transaction/"
  );

  // -- GOLD LAYER --
  hudiService.createDatabase(
    datalakeConf.goldDb,
    s"s3a://${configVars.BUCKET}/gold/gold_db/"
  );
  hudiService.createGoldDimCustomer(
    datalakeConf.goldDb,
    datalakeConf.dimCustomer,
    s"s3a://${configVars.BUCKET}/gold/dim_customer"
  );
  hudiService.createGoldDimProduct(
    datalakeConf.goldDb,
    datalakeConf.dimProduct,
    s"s3a://${configVars.BUCKET}/gold/dim_product"
  );
  hudiService.createDimDate(
    datalakeConf.goldDb,
    datalakeConf.dimDate,
    s"s3a://${configVars.BUCKET}/gold/dim_date"
  );
  hudiService.createFactTransaction(
    datalakeConf.goldDb,
    datalakeConf.factTransaction,
    s"s3a://${configVars.BUCKET}/gold/fact_transaction"
  );

  // Populate dim_date (50 years of dates, runs once)
  transformDimDate.populateDimDate();

  // ══════════════════════════════════════════════════════════════════════════
  // Extract from Kafka → RAW LAYER
  // ══════════════════════════════════════════════════════════════════════════
  var customerStreamDf =
    kafkaExtractor.extractStreamKafka(topic = configVars.RAW_CUSTOMER_TOPIC);
  var productStreamDf =
    kafkaExtractor.extractStreamKafka(configVars.PRODUCT_TOPIC);
  var eventStreamDf = kafkaExtractor.extractStreamKafka(configVars.EVENT_TOPIC);
  var transactionDf =
    kafkaExtractor.extractStreamKafka(configVars.TRANSACTION_TOPIC);

  // Load data into raw_table
  val combinedRawDf = customerStreamDf
    .union(productStreamDf)
    .union(eventStreamDf)
    .union(transactionDf)

  hudiService.writeRaw(combinedRawDf, "all_topics_raw")

  // ══════════════════════════════════════════════════════════════════════════
  // SILVER LAYER — Cleansed data (no SCD2)
  // ══════════════════════════════════════════════════════════════════════════
  var rawDf = combinedRawDf;

  // Silver Customer
  var stgCustomerSilver: DataFrame = transformCustomerSilver.stgSilver(rawDf);

  // Silver Transaction
  var stgTransaction =
    transformTransactionSilver.stgSilver(rawDf);

  // Extract unknown customers from transaction and create new customer records
  var unknownCustomers = stgTransaction
    .filter(col("is_new_customer") === true)
    .select("customer_id", "phone_number")
    .withColumn("first_name", lit("Unknown"))
    .withColumn("last_name", lit("Unknown"))
    .withColumn("gender", lit("Unknown"))
    .withColumn("date_of_birth", lit(null).cast("date"))
    .withColumn("email", lit("Unknown"))
    .withColumn("country", lit("Unknown"))
    .withColumn("customer_creation_date", current_date())

  var combinedCustomerSilver =
    stgCustomerSilver.unionByName(unknownCustomers, allowMissingColumns = true)

  // Silver customer (từ CDC + unknown) — stream riêng, checkpoint riêng
  sqlService.upsertSilverCustomer(
    combinedCustomerSilver,
    s"${datalakeConf.silverDb}.${datalakeConf.silverCustomerTable}"
  )

  // Silver Product
  var productDf: DataFrame = rawDf.filter(col("key") === "product");
  var stgProductSilver: DataFrame = transformProduct.stgSilver(productDf);
  sqlService.upsertSilverProduct(
    stgProductSilver,
    s"${datalakeConf.silverDb}.${datalakeConf.silverProductTable}"
  );

  sqlService.insertTransaction(
    stgTransaction,
    s"${datalakeConf.silverDb}.${datalakeConf.silverTransactionTable}"
  )

  // ══════════════════════════════════════════════════════════════════════════
  // GOLD LAYER — Star Schema (SCD2 dimensions + fact)
  // ══════════════════════════════════════════════════════════════════════════

  // Dim Customer (SCD2): silver_customer → gold.dim_customer
  var stgDimCustomer: DataFrame =
    transformDimCustomer.stgDimCustomer(combinedCustomerSilver);
  sqlService.mergeGoldDimCustomer(
    stgDimCustomer,
    s"${datalakeConf.goldDb}.${datalakeConf.dimCustomer}"
  )

  // Dim Product (SCD2): silver_product → gold.dim_product
  var stgDimProduct: DataFrame =
    transformDimProduct.stgDimProduct(stgProductSilver);
  sqlService.mergeGoldDimProduct(
    stgDimProduct,
    s"${datalakeConf.goldDb}.${datalakeConf.dimProduct}"
  )

  val hudiOptions = Map(
    "hoodie.table.name" -> datalakeConf.factTransaction,
    "hoodie.datasource.write.recordkey.field" -> "transaction_sk",
    "hoodie.datasource.write.precombine.field" -> "time_stamp",
    "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
    "hoodie.metadata.enable" -> "false"
  )

  // Fact Transaction: silver_transaction + dim_customer + dim_product + dim_date → gold.fact_transaction
  sqlService.writeFactTransaction(
    stgTransaction, // DataFrame có is_new_customer flag, chưa drop
    s"${datalakeConf.goldDb}.${datalakeConf.dimCustomer}",
    s"${datalakeConf.goldDb}.${datalakeConf.dimProduct}",
    s"${datalakeConf.goldDb}.${datalakeConf.dimDate}",
    s"${datalakeConf.silverDb}.${datalakeConf.silverTransactionTable}",
    s"s3a://${configVars.BUCKET}/gold/fact_transaction",
    hudiOptions, // Map các config Hudi của bạn
    s"s3a://${configVars.CHECKPOINT_BUCKET}/${configVars.CHECKPOINT_FOLDER}/fact_transaction"
  )

  // ══════════════════════════════════════════════════════════════════════════
  // SERVING LAYER — MongoDB unified_profile
  // ══════════════════════════════════════════════════════════════════════════

  // Event activity (from stgEvent) → unified_profile.activity_log
  var stgEvent: DataFrame = transformEvent.transformStgEvent(rawDf);
  mongodbService.writeMongoDb(
    stgEvent,
    configVars.ATLAS_DATABASE,
    configVars.ATLAS_COLLECTION,
    configVars.atlasConnectionString(),
    "customer_id",
    s"${configVars.CHECKPOINT_MONGODB_EVENT}/01"
  );

  // Transaction log → unified_profile.transaction_log
  var resolvedTransaction = transformTransactionSilver.resolve(stgTransaction);
  mongodbService.writeMongoDb(
    resolvedTransaction,
    configVars.ATLAS_DATABASE,
    configVars.ATLAS_COLLECTION,
    configVars.atlasConnectionString(),
    "customer_id",
    s"${configVars.CHECKPOINT_MONGODB_EVENT}/02"
  );

  // Customer demographics → unified_profile
  mongodbService.writeMongoDb(
    stgCustomerSilver,
    configVars.ATLAS_DATABASE,
    configVars.ATLAS_COLLECTION,
    configVars.atlasConnectionString(),
    "customer_id",
    configVars.CHECKPOINT_MONGODB_CUSTOMER
  );

  spark.streams.awaitAnyTermination();
}
