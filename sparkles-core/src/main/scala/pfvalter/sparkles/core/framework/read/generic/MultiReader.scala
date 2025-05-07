package pfvalter.sparkles.core.framework.read.generic

import org.apache.spark.sql.SparkSession
import shapeless._

/**
 * The ordering in files (or sources) and readers needs to be properly aligned with the readers.
 *   The typed implementation on the core and SDK will have logic that makes sure of it at compile or
 *   instantiation time.
 */
case class MultiReader(
  readers: HList
)(
  implicit val spark: SparkSession
) extends Read {

  /*
   * Need to get rid of this method or the Any at some point
   */
  private def toInputHListRecursive(xs: List[Any]): HList = xs match {
    case Nil => HNil
    case _ => xs.head :: toInputHListRecursive(xs.tail)
  }

  /**
   * Recursive aux method that unwraps the HList.
   * @param xs rest of the HList
   * @return A List of Readers (works for inputs with DSs or DFs or even mixed ones too!)
   */
  private def fromHListRecursive(xs: HList): List[ReaderWithSource[_]] = xs match {
    case HNil => Nil
    case head :: tail => head match {
      case h: ReaderWithSource[_] => h :: fromHListRecursive(tail)
      case _ => fromHListRecursive(tail)
    }
  }

  /**
   * Multi Read Implementation
   * @tparam I the HList type for the input after it is read.
   * @return all inputs read.
   */
  override def read[I <: HList]: () => I = () => {
    val readersList = fromHListRecursive(readers)
    val fullRead = readersList.map(_.readHead)
    toInputHListRecursive(fullRead)
  }.asInstanceOf[I]
}
