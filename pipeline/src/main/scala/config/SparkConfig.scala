package config
import org.apache.spark.SparkConf

class SparkConfig(master : String, appName : String = "pipeline"){
    var conf : SparkConf = new SparkConf().setMaster(master).setAppName(appName);

    def configS3(s3AccessKey : String, s3SecretKey : String): Unit = {
        this.conf.set("spark.sql.extensions","org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                .set("'spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .set("spark.hadoop.fs.s3a.access.key", s3AccessKey)
                .set("spark.hadoop.fs.s3a.secret.key", s3SecretKey)
                .set("spark.hadoop.fs.s3a.path.style.access", "true")
                .set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
                .set("fs.s3a.connection.establish.timeout", "15000")
                .set("spark.kryo.registrator","org.apache.spark.HoodieSparkKryoRegistrar")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    }

    def configNeo4j(neo4jUrl : String, username : String, password : String, dbName : String): Unit = {
        this.conf.set("neo4j.url", neo4jUrl)
            .set("neo4j.authentication.basic.username", username)
            .set("neo4j.authentication.basic.password", password)
            .set("neo4j.database", dbName);
    }

    def getConf() : SparkConf = {
        return this.conf;
    }
}