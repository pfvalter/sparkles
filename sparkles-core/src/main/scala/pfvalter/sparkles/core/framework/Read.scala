package pfvalter.sparkles.core.framework

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import shapeless.HList

import scala.reflect.runtime.universe.TypeTag

/*
 * Basic abstract ReadType "carrier" trait
 */
trait Reader {
  type InputType

  def read[U <: HList]: () => U
}

trait SingleReader[T <: Product] extends Reader {
  override type InputType = T

  implicit val readTypeTag: TypeTag[T]

  implicit val spark: SparkSession

  implicit val readEncoder: Encoder[T] = Encoders.product[T]
}