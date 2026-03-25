package config
import org.apache.spark.SparkConf

class SparkConfig(master : String, appName : String = "pipeline"){
    var conf : SparkConf = new SparkConf().setMaster(master).setAppName(appName);

    def configS3(s3AccessKey : String, s3SecretKey : String): Unit = {
        this.conf.set("spark.sql.extensions","org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .set("spark.hadoop.fs.s3a.access.key", s3AccessKey)
                .set("spark.hadoop.fs.s3a.secret.key", s3SecretKey)
                .set("spark.hadoop.fs.s3a.path.style.access", "true")
                .set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
                .set("fs.s3a.connection.establish.timeout", "15000")
                .set("spark.kryo.registrator","org.apache.spark.HoodieSparkKryoRegistrar")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("hive.metastore.uris", "thrift://metastore:9083")
                .set("spark.sql.catalogImplementation", "hive")
                .set("fs.s3a.impl.disable.cache", "true")
                .set("fs.automatic.close", "false")
                .set("spark.hadoop.fs.s3a.connection.maximum", "500")
                .set("spark.hadoop.fs.s3a.threads.max", "256")
                .set("spark.hadoop.fs.s3a.impl.disable.cache", "true")
                .set("spark.hadoop.fs.automatic.close", "false")
    }

    def getConf() : SparkConf = {
        return this.conf;
    }
}