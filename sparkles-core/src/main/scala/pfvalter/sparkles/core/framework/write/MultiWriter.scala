package pfvalter.sparkles.core.framework.write

import org.apache.spark.sql.{Dataset, SparkSession}
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
  private def fromOutputHListRecursive(xs: HList): List[Dataset[_ <: Product]] = xs match {
    case HNil => Nil
    case head :: tail => head.asInstanceOf[Dataset[_ <: Product]] :: fromOutputHListRecursive(tail)
  }

  private def fromHListRecursive(xs: HList): List[Writer[_ <: Product]] = xs match {
    case HNil => Nil
    case head :: tail => head.asInstanceOf[Writer[_ <: Product]] :: fromHListRecursive(tail)
  }

  override def write[U <: HList](output: U): U = {
    val outputList: Seq[Dataset[_ <: Product]] = fromOutputHListRecursive(output)
    val writersList: Seq[Writer[_ <: Product]] = fromHListRecursive(writers)
    val zippedList: Seq[(Writer[_ <: Product], Dataset[_ <: Product])] = writersList.zip(outputList)

    Try {
      zippedList.map{case (write, out) => write.writeHead(out.asInstanceOf[Dataset[write.OutputType]])}
    }

    output
  }
}
