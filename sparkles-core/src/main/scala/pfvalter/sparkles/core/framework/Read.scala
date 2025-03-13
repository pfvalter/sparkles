package pfvalter.sparkles.core.framework

import org.apache.spark.sql.Encoder

/*
 * Basic abstract ReadType "carrier" trait
 */
trait Read {
  type ReadType
  type InputType
}

/*
 * Basic abstract Reader trait
 */
trait Reader extends Read {
  val read: () => ReadType
  implicit val readEncoder: Encoder[InputType]
}