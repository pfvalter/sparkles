package pfvalter.sparkles.core.framework

import org.apache.spark.sql.Encoder

/*
 * Basic abstract WriteType "carrier" trait
 */
trait Write {
  type WriteType
  type OutputType
}

/*
 * Basic abstract Writer trait
 */
trait Writer extends Write {
  val write: WriteType => WriteType
  implicit val writeEncoder: Encoder[OutputType]
}

