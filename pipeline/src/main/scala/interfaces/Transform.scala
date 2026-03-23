package interfaces

import org.apache.spark.sql.DataFrame

trait Transform {
    def stgSilver(df : DataFrame): DataFrame;
    def stgGold(df : DataFrame): DataFrame;
}
