package pfvalter.sparkles.core.io.read

import org.apache.spark.sql.{Encoder, SparkSession}
import pfvalter.sparkles.core.io.format._
import pfvalter.sparkles.core.framework.SingleReader
import shapeless.{HList, HNil}

import scala.reflect.runtime.universe.TypeTag

/**
 * -- fileFormat: FileFormat --
 * File Format of the file.
 * Note: Currently has no default. Later, a file type inference will fix that.
 */
case class SingleReaderFromFile[T <: Product](
  //Implement readFrom later, now let's just use Paths as Strings
  // -> val readFrom: FileLocation
  //This val is only here temporarily to hold the filePath
  filePath: String,
  fileFormat: FileFormat
)(
  implicit val spark: SparkSession,
  implicit val readTypeTag: TypeTag[T]
) extends SingleReader[T] {

  override def read[R <: HList](implicit readEncoder: Encoder[T]): () => R = () => {
    fileFormat match {
      case JSON => spark.read.json(filePath).as[T] :: HNil
      case PARQUET => spark.read.parquet(filePath).as[T] :: HNil
      case CSV => spark.read.csv(filePath).as[T] :: HNil
      case TEXT => spark.read.text(filePath).as[T] :: HNil
      case _ => throw new Exception("File Format not defined")
    }
  }.asInstanceOf[R]
}
