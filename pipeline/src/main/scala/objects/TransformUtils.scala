package objects

import org.apache.spark.sql.expressions.UserDefinedFunction
import java.util.Base64
import java.math.{BigInteger, BigDecimal}
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
}
