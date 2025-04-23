package pfvalter.sparkles.core.io.read

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import pfvalter.sparkles.core.io.format._
import pfvalter.sparkles.core.framework.SingleReader
import shapeless.{HList, HNil}

import scala.reflect.runtime.universe.TypeTag

case class SingleReaderFromFile[T <: Product](
  file: FileMetadata
)(
  implicit val spark: SparkSession,
  implicit val readTypeTag: TypeTag[T]
) extends SingleReader[T] {

  lazy val readHead: Dataset[T] = file.fileFormat match {
    case JSON => spark.read.json(file.filePath).as[T]
    case PARQUET => spark.read.parquet(file.filePath).as[T]
    case CSV => spark.read.csv(file.filePath).as[T]
    case TEXT => spark.read.text(file.filePath).as[T]
    case _ => throw new Exception("File Format not defined")
  }

  override def read[U <: HList]: () => U = () => {
    readHead :: HNil
  }.asInstanceOf[U]
}

object SingleReaderFromFile {
  def fromSingleReader[T <: Product](
    file: FileMetadata
  )(implicit spark: SparkSession, readTypeTag: TypeTag[T]): SingleReaderFromFile[T] = {
    SingleReaderFromFile[T](file)
  }
}
