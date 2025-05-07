package pfvalter.sparkles.core.framework.read

import org.apache.spark.sql.{Dataset, SparkSession}
import pfvalter.sparkles.core.framework.read.generic.ReaderTrait
import shapeless._

/**
 * The ordering in files (or sources) and readers needs to be properly aligned with the readers.
 *   The typed implementation on the core and SDK will have logic that makes sure of it at compile or
 *   instantiation time.
 */
case class MultiReader[R <: HList](
  readers: R
)(
  implicit val spark: SparkSession
) extends Read {

  private def toInputHListRecursive(xs: List[Any]): HList = xs match {
    case Nil => HNil
    case _ => xs.head :: toInputHListRecursive(xs.tail)
  }

  private def fromHListRecursive(xs: HList): List[ReaderTrait] = xs match {
    case HNil => Nil
    case head :: tail => head match {
      case h: ReaderTrait => h :: fromHListRecursive(tail)
      case _ => fromHListRecursive(tail)
    }
  }

  override def read[U <: HList]: () => U = () => {
    val readersList = fromHListRecursive(readers)
    val fullRead = readersList.map(_.readHead)
    toInputHListRecursive(fullRead)
  }.asInstanceOf[U]
}
