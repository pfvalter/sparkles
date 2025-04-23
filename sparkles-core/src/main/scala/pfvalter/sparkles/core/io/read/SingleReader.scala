package pfvalter.sparkles.core.io.read

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import pfvalter.sparkles.core.framework.Reader

import scala.reflect.runtime.universe.TypeTag

trait SingleReader[T <: Product] extends Reader {
  override type InputType = T

  implicit val readTypeTag: TypeTag[T]

  implicit val spark: SparkSession

  implicit val readEncoder: Encoder[T] = Encoders.product[T]
}
