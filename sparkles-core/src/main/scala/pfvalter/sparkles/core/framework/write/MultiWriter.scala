package pfvalter.sparkles.core.framework.write

import org.apache.spark.sql.{Dataset, SparkSession}
import pfvalter.sparkles.core.framework.write.generic.WriterTrait
import shapeless._

import scala.util.Try

/**
 * The ordering in files (or sources) and writers needs to be properly aligned with the writers.
 *   The typed implementation on the core and SDK will have logic that makes sure of it at compile or
 *   instantiation time.
 */
case class MultiWriter[W <: HList](
  writers: W
)(
  implicit val spark: SparkSession
) extends Write {

  // Consider using mappers here too...
  private def fromOutputHListRecursive(xs: HList): List[Any] = xs match {
    case HNil => Nil
    case head :: tail => head :: fromOutputHListRecursive(tail)
  }

  private def fromHListRecursive(xs: HList): List[WriterTrait] = xs match {
    case HNil => Nil
    case head :: tail => head.asInstanceOf[WriterTrait] :: fromHListRecursive(tail)
  }

  override def write[U <: HList](output: U): U = {
    val outputList: Seq[Any] = fromOutputHListRecursive(output)
    val writersList: Seq[WriterTrait] = fromHListRecursive(writers)
    val zippedList: Seq[(WriterTrait, Any)] = writersList.zip(outputList)

    Try {
      zippedList.map{case (write, out) => write.writeHead(out.asInstanceOf[write.WriteAs])}
    }

    output
  }
}
