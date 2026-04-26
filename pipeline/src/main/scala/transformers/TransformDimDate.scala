package transformers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.{ConfigVariables, DatalakeConfig}

/** Generates and populates the dim_date dimension table.
  * Pre-populates 50 years of dates (25 years back + 25 years forward from today).
  */
class TransformDimDate(spark: SparkSession) {
  private val datalakeConf = new DatalakeConfig()
  private val configVars = new ConfigVariables()

  /** Populates dim_date if it is empty.
    * Generates dates from (today - 25 years) to (today + 25 years).
    * date_key format: YYYYMMDD as integer (e.g. 20260426)
    */
  def populateDimDate(): Unit = {
    val dimDateTable =
      s"${datalakeConf.goldDb}.${datalakeConf.dimDate}"
    val dimDatePath =
      s"s3a://${configVars.BUCKET}/gold/dim_date"

    // Check if dim_date already has data
    val count = spark.table(dimDateTable).count()
    if (count > 0) {
      print(s"=========dim_date already populated with $count rows============")
      return
    }

    import spark.implicits._

    // Generate 50 years of dates: 25 years back to 25 years forward
    val today = java.time.LocalDate.now()
    val startDate = today.minusYears(25)
    val endDate = today.plusYears(25)

    val totalDays =
      java.time.temporal.ChronoUnit.DAYS.between(startDate, endDate).toInt

    val dates = (0 to totalDays).map { i =>
      val d = startDate.plusDays(i)
      (
        d.getYear * 10000 + d.getMonthValue * 100 + d.getDayOfMonth, // date_key
        java.sql.Date.valueOf(d), // date
        d.getYear, // year
        d.getMonthValue, // month
        d.getDayOfMonth, // day
        d.getDayOfWeek.getValue, // day_of_week (1=Monday..7=Sunday)
        (d.getMonthValue - 1) / 3 + 1 // quarter
      )
    }

    val dimDateDf = dates
      .toDF("date_key", "date", "year", "month", "day", "day_of_week", "quarter")

    // Write batch to Hudi table
    dimDateDf.write
      .format("hudi")
      .option("hoodie.table.name", datalakeConf.dimDate)
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.recordkey.field", "date_key")
      .option("hoodie.datasource.write.precombine.field", "date_key")
      .option("hoodie.metadata.enable", "false")
      .option(
        "hoodie.datasource.write.payload.class",
        "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
      )
      .mode("append")
      .save(dimDatePath)

    print(s"=========Populated dim_date with ${dates.size} rows============")
  }
}
