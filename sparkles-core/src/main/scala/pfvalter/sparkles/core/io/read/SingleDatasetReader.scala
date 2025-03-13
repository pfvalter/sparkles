package pfvalter.sparkles.core.io.read

import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import pfvalter.sparkles.core.framework.Reader

import scala.reflect.runtime.universe.TypeTag

trait SingleDatasetReader[T <: Product] extends Reader {
  override type InputType = T
  override type ReadType = Dataset[T]

  override val read: () => Dataset[T] = ???

  implicit val readTypeTag: TypeTag[T]
  override implicit val readEncoder: Encoder[T] = Encoders.product[T]
}
