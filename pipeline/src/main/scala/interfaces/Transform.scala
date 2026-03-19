package interfaces

import org.apache.spark.sql.DataFrame

trait Transform {
    def transformFromRaw(rawDf : DataFrame): DataFrame;
    def transformFromSilver(silverDf : DataFrame): DataFrame;
}
