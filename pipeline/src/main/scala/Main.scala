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

object Main extends App {
    // Base variables
    val configVars = new ConfigVariables()
    val datalakeConf = new DatalakeConfig()

    //region Spark
    var sparkConf = new SparkConfig("spark://master:7077")
    sparkConf.configS3(configVars.S3_ACCESS_KEY, configVars.S3_SECRET_KEY)
    // sparkConf.configNeo4j(configVars.NEO4J_URL, configVars.NEO4J_USERNAME, configVars.NEO4J_PASSWORD, configVars.NEO4J_DBNAME)
    
    val spark = SparkSession.builder().config(sparkConf.getConf()).enableHiveSupport().getOrCreate();

    spark.sparkContext.setLogLevel("WARN");
    import spark.implicits._
    //endregion

    // region Initial dependencies
    var hudiService : HudiService = new HudiService(spark);
    var kafkaExtractor: ExtractKafka = new ExtractKafka(spark, configVars.KAFKA_BOOTSTRAP);
    var transformCustomerSilver : TransformCustomerSilver = new TransformCustomerSilver();
    var sqlService : SqlStreamService = new SqlStreamService();
    // var transformTransactionSilver : TransformTransactionSilver = new TransformTransactionSilver();
    // endregion

    // Initiate database
    hudiService.createDatabase(datalakeConf.rawDb, s"s3a://${configVars.BUCKET}/bronze/raw_db/");
    hudiService.createRawTable(datalakeConf.rawDb, datalakeConf.rawTable, s"s3a://${configVars.BUCKET}/bronze/raw_table/");

    hudiService.createDatabase(datalakeConf.silverDb,  s"s3a://${configVars.BUCKET}/silver/silver_db/");
    hudiService.createSilverTransaction(datalakeConf.silverDb, datalakeConf.silverTransactionTable, s"s3a://${configVars.BUCKET}/silver/silver_transaction/");
    hudiService.createSilverCustomer(datalakeConf.silverDb, datalakeConf.silverCustomerTable, s"s3a://${configVars.BUCKET}/silver/silver_customer");
    hudiService.createDimProduct(datalakeConf.silverDb, datalakeConf.dimProduct, s"s3a://${configVars.BUCKET}/silver/dim_product");

    // Extract   
    var customerStreamDf = kafkaExtractor.extractStreamKafka(topic = configVars.RAW_CUSTOMER_TOPIC);

    var unionDf = customerStreamDf;
    // Load
    // Load data into raw
    hudiService.writeStream(unionDf, 
                            configVars.CHECKPOINT_BRONZE, 
                            datalakeConf.rawDb, 
                            datalakeConf.rawTable, 
                            s"s3a://${configVars.BUCKET}/bronze/raw_table/");

    // Transform
    var rawDf = hudiService.readStreamTable(s"s3a://${configVars.BUCKET}/bronze/raw_table/");
    // rawDf.writeStream.format("console").start(); // for debug
    var customerDf = rawDf.filter(col("key") === "customer");

    var stgCustomerSilver : DataFrame = transformCustomerSilver.stgSilver(customerDf);
    sqlService.mergeCustomerSilver(stgCustomerSilver, s"${datalakeConf.silverDb}.${datalakeConf.silverCustomerTable}")
    // stgCustomerSilver.writeStream.format("console").start();

    // Product
    

    spark.streams.awaitAnyTermination();
}