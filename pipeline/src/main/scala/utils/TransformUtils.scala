package utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction

import java.util.Base64
import java.math.{BigDecimal, BigInteger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object TransformUtils {
  private val SCALE = 2

  // Dùng cú pháp Typed UDF để vừa sửa lỗi UNTYPED vừa an toàn serialization
  val decodeDecimalUdf: UserDefinedFunction = udf[java.math.BigDecimal, String]((base64Str: String) => {
    if (base64Str == null) null
    else {
      val bytes      = Base64.getDecoder.decode(base64Str)
      val unscaled   = new BigInteger(bytes)
      new BigDecimal(unscaled, SCALE)
    }
  })

  def normalizePhoneNumber(df: DataFrame, phoneCol: String = "phone_number"): DataFrame = {
    var df2 = df.withColumn(phoneCol, regexp_replace(col(phoneCol), "[\\s\\(\\)\\-]", ""))
      .withColumn(phoneCol, regexp_replace(col(phoneCol), "^[+84]", "0"));

    return df2;
  }
}
