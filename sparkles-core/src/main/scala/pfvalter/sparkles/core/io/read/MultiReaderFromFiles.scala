package pfvalter.sparkles.core.io.read

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import pfvalter.sparkles.core.framework.{Reader, SingleReader}
import pfvalter.sparkles.core.io.format._
import shapeless.{HList, HNil}

import scala.reflect.runtime.universe.TypeTag

/**
 * -- fileFormat: FileFormat --
 * The ordering in files and readers needs to be properly aligned
 */
case class MultiReaderFromFiles(
  readers: Seq[SingleReaderFromFile[_]]
)(
  implicit val spark: SparkSession
) extends Reader {

  private def toHListRecursive(xs: List[Any]): HList = xs match {
    case Nil => HNil
    case head :: tail => head :: toHListRecursive(tail)
  }

  override def read[U <: HList]: () => U = () => {
    val fullRead: Seq[Dataset[_]] = readers.map(_.readHead)
    toHListRecursive(fullRead.toList)
  }.asInstanceOf[U]
}
