package pfvalter.sparkles.core.framework

import org.apache.spark.sql.Encoder
import shapeless.HList
import shapeless.ops.hlist.IsHCons

/*
 * Basic abstract ReadType "carrier" trait
 */
trait Read {
  type InputType
  def read[R <: HList](implicit readEncoder: Encoder[InputType]): () => R
}

/*
 * Basic abstract Reader trait
 */
trait Reader extends Read {
  implicit val readEncoder: Encoder[InputType]
}