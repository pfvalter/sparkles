package pfvalter.sparkles.core.framework

import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import pfvalter.sparkles.core.io.format.{CSV, JSON, PARQUET, TEXT}
import pfvalter.sparkles.core.io.source.{DataSource, FILE}
import shapeless.{HList, HNil}

import scala.reflect.runtime.universe.TypeTag

// This has to be a regular class to be extended by the FileReader.
//   Later on, untangle to allow case class here
class Reader[T <: Product](
  fromSource: DataSource
)(
  implicit val spark: SparkSession,
  implicit val readTypeTag: TypeTag[T]
) extends Read {
  override type InputType = T

  implicit val readEncoder: Encoder[T] = Encoders.product[T]

  lazy val readHead: Dataset[T] = fromSource match {
    case file: FILE => FileReader[T](file).readHead
  }

  override def read[U <: HList]: () => U = fromSource match {
    case file: FILE => FileReader[T](file).read
  }
}

private case class FileReader[T <: Product](
  file: FILE
)(
  implicit spark: SparkSession, readTypeTag: TypeTag[T]
) extends Reader[T](file) {

  override lazy val readHead: Dataset[T] = file.fileFormat match {
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