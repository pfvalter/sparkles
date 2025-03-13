package pfvalter.sparkles.core.io.write

import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import pfvalter.sparkles.core.framework.Writer

import scala.reflect.runtime.universe.TypeTag

trait SingleDatasetWriter[T <: Product] extends Writer {
  override type OutputType = T
  override type WriteType = Dataset[T]

  override val write: Dataset[T] => Dataset[T]

  implicit val writeTypeTag: TypeTag[T]
  override implicit val writeEncoder: Encoder[T] = Encoders.product[T]
}
