package config

class ConfigVariables {
    val S3_ACCESS_KEY : String = "admin";
    val S3_SECRET_KEY : String = "password";
    val BUCKET : String = "tables";
    val CHECKPOINT_BUCKET : String = "lake";
    val CHECKPOINT_FOLDER : String = "checkpoints"


    val KAFKA_BOOTSTRAP : String = "kafka:29092";
    val RAW_2_BRONZE_TOPIC : String = "raw-2-bronze-topic";
    val BRONZE_2_SILVER_TOPIC :  String = "bronze-2-silver";
    val SILVER_2_GOLD_TOPIC : String = "silver-2-gold";
    val RAW_CUSTOMER_TOPIC : String = "e-commerce-customer.public.customer";
    val PRODUCT_TOPIC : String = "e-commerce-product.public.product";
    val EVENT_TOPIC : String = "e-commerce-event.public.event";

    val CHECKPOINT_BRONZE : String = s"s3a://$CHECKPOINT_BUCKET/$CHECKPOINT_FOLDER/bronze_raw/";
    val CHECKPOINT_SILVER : String = s"s3a://$CHECKPOINT_BUCKET/$CHECKPOINT_FOLDER/silver_transaction/";
    val CHECKPOINT_MONGODB_CUSTOMER : String = s"s3a://$CHECKPOINT_BUCKET/$CHECKPOINT_FOLDER/mongodb/customer";
    val CHECKPOINT_MONGODB_EVENT : String = s"s3a://$CHECKPOINT_BUCKET/$CHECKPOINT_FOLDER/mongodb/event";

    val ATLAS_USERNAME : String = System.getenv("ATLAS_USERNAME");
    val ATLAS_PASSWORD : String = System.getenv("ATLAS_PASSWORD");
    val ATLAS_CLUSTER : String = System.getenv("ATLAS_CLUSTER");
    val ATLAS_DATABASE : String = System.getenv("ATLAS_DATABASE");
    val ATLAS_COLLECTION : String = System.getenv("ATLAS_COLLECTION");
    val ATLAS_APP_NAME : String = System.getenv("ATLAS_APP_NAME");

    def atlasConnectionString(): String = {
        return s"mongodb+srv://${ATLAS_USERNAME}:${ATLAS_PASSWORD}@${ATLAS_CLUSTER}/?appName=${ATLAS_APP_NAME}";
    }
}
