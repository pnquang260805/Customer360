package transformers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import interfaces.Transform

class TransformTransactionSilver extends Transform {
    def stgSilver(rawDf: DataFrame): DataFrame = {
        // Khai báo Schema dựa trên JSON sinh ra từ prepare_data/transaction.py
        // Dữ liệu này là JSON phẳng, không có màng bọc "after" như CDC Debezium
        val dataSchema = StructType(Seq(
            StructField("transaction_id", StringType, true),
            StructField("customer_id", StringType, true),
            StructField("product_id", StringType, true),       // HudiService khai báo là STRING
            StructField("product_name", StringType, true),
            StructField("price", DecimalType(10, 2), true),
            StructField("quantity", DecimalType(10, 2), true), // HudiService khai báo là DECIMAL(10,2)
            StructField("total_amount", DecimalType(10, 2), true),
            StructField("event_time", StringType, true)        // Python gửi lên chuẩn ISO Format (String)
        ));

        // 1. Lấy đúng luồng dữ liệu transaction
        var rawTransaction : DataFrame = rawDf.where(col("key") === "transaction").select("value");
        
        // 2. Ép kiểu JSON String về Struct
        var transactionDf : DataFrame = rawTransaction.select(from_json(col("value").cast(StringType), dataSchema).alias("data"));
        
        // 3. Trải phẳng (flatten) các cột ra ngoài
        var explodedDf : DataFrame = transactionDf.select(col("data.*"));

        // 4. Lọc bỏ các dòng lỗi hoặc sinh ra do noise
        var finalDf : DataFrame = explodedDf.filter(col("transaction_id").isNotNull);

        return finalDf;
    }    
    
    def stgGold(silverDf: DataFrame): DataFrame = {
        return null;
    }
}
