package pfvalter.sparkles.core.framework.write.generic

import org.apache.spark.sql.SparkSession
import shapeless._

import scala.util.Try

/**
 * The ordering in files (or sources) and writers needs to be properly aligned with the writers.
 *   The SDK will have logic that makes sure of it at compile or instantiation time.
 */
case class MultiWriter(
  writers: HList
)(
  implicit val spark: SparkSession
) extends Write {

  // Consider using HList mappers instead to get read of this method...
  private def fromOutputHListRecursive(xs: HList): List[Any] = xs match {
    case HNil => Nil
    case head :: tail => head :: fromOutputHListRecursive(tail)
  }

  private def fromHListRecursive(xs: HList): List[Writer[_]] = xs match {
    case HNil => Nil
    case head :: tail => head match {
      case h: Writer[_] => h :: fromHListRecursive(tail)
    }
  }

  /**
   * Multi Writer write implementation
   * @tparam O the HList type for the output after it is read.
   * @return all outputs written.
   */
  override def write[O <: HList](output: O): O = {
    val outputList: Seq[Any] = fromOutputHListRecursive(output)
    val writersList: Seq[Writer[_]] = fromHListRecursive(writers)
    val zippedList: Seq[(Writer[_], Any)] = writersList.zip(outputList)

    Try {
      zippedList.map{case (write, out) => write.writeHead(out.asInstanceOf[write.WriteAs])}
    }

    output
  }
}
