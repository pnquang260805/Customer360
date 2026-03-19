package config

class ConfigVariables {
    val S3_ACCESS_KEY : String = "admin";
    val S3_SECRET_KEY : String = "password";

    val KAFKA_BOOTSTRAP : String = "kafka:29092";

    val NEO4J_URL : String = "bolt://neo4j:7687";
    val NEO4J_USERNAME : String = "neo4j";
    val NEO4J_PASSWORD : String = "capstoneptit";
    val NEO4J_DBNAME : String = "identity-graph";
    val BUCKET : String = "tables";

    val RAW_2_BRONZE_TOPIC : String = "raw-2-bronze-topic";
    val BRONZE_2_SILVER_TOPIC :  String = "bronze-2-silver";
    val SILVER_2_GOLD_TOPIC : String = "silver-2-gold";
    val RAW_CUSTOMER_TOPIC : String = "e-commerce-customer.public.customer";

    val CHECKPOINT_BRONZE : String = s"s3a://$BUCKET/checkpoints/bronze_raw/";
    val CHECKPOINT_SILVER : String = s"s3a://$BUCKET/checkpoints/silver_transaction/";

}
