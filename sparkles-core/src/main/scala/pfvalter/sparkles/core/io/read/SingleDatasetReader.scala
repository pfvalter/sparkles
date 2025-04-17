package pfvalter.sparkles.core.io.read

import org.apache.spark.sql.{Encoder, Encoders}
import pfvalter.sparkles.core.framework.Reader
import shapeless.HList

import scala.reflect.runtime.universe.TypeTag

trait SingleDatasetReader[T <: Product] extends Reader {
  override type InputType = T
  implicit val readTypeTag: TypeTag[T]

  implicit val readEncoder: Encoder[T] = Encoders.product[T]

  override def read[R <: HList](implicit readEncoder: Encoder[T]): () => R
}
