package interfaces

import org.apache.spark.sql.DataFrame


trait Extract{
    def extract(): DataFrame;
}