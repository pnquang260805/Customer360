package config

class ConfigVariables {
    val S3_ACCESS_KEY : String = "admin";
    val S3_SECRET_KEY : String = "password";
    val BUCKET : String = "tables";


    val KAFKA_BOOTSTRAP : String = "kafka:29092";
    val RAW_2_BRONZE_TOPIC : String = "raw-2-bronze-topic";
    val BRONZE_2_SILVER_TOPIC :  String = "bronze-2-silver";
    val SILVER_2_GOLD_TOPIC : String = "silver-2-gold";
    val RAW_CUSTOMER_TOPIC : String = "e-commerce-customer.public.customer";
    val PRODUCT_TOPIC : String = "e-commerce-product.public.product";
    val EVENT_TOPIC : String = "e-commerce-event.public.event";

    val CHECKPOINT_BRONZE : String = s"s3a://$BUCKET/checkpoints/bronze_raw/";
    val CHECKPOINT_SILVER : String = s"s3a://$BUCKET/checkpoints/silver_transaction/";
}
