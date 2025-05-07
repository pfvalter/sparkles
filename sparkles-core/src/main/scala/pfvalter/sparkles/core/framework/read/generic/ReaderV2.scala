package pfvalter.sparkles.core.framework.read.generic

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, Encoder, Encoders, SparkSession}
import pfvalter.sparkles.core.framework.read.Read
import pfvalter.sparkles.core.io.format.{CSV, JSON, PARQUET, TEXT}
import pfvalter.sparkles.core.io.source.{DataSource, FILE}
import shapeless.{HList, HNil}

import scala.reflect.runtime.universe.TypeTag

trait ReaderTrait extends Read {
  val fromSource: DataSource

  implicit val sparkSession: SparkSession

  type ReadAs

  def fileReader(file: FILE): GenericFileReader

  def readHead: ReadAs = fromSource match {
    case file: FILE => fileReader(file).readHead.asInstanceOf[ReadAs]
  }

  override def read[U <: HList]: () => U = fromSource match {
    case file: FILE => fileReader(file).read
  }
}

abstract class GenericFileReader(
  file: FILE
)(
  override implicit val sparkSession: SparkSession
) extends ReaderTrait {
  override lazy val fromSource: DataSource = file

  val schema: Option[StructType]

  lazy val dfReader: DataFrameReader = schema match {
    case Some(schema) => sparkSession.read.schema(schema)
    case None         => sparkSession.read
  }

  override def readHead: ReadAs = file.fileFormat match {
    case JSON => dfReader.json(file.filePath).asInstanceOf[ReadAs]
    case PARQUET => dfReader.parquet(file.filePath).asInstanceOf[ReadAs]
    case CSV => dfReader.csv(file.filePath).asInstanceOf[ReadAs]
    case TEXT => dfReader.text(file.filePath).asInstanceOf[ReadAs]
    case _ => throw new Exception("File Format not defined")
  }

  override def read[U <: HList]: () => U = () => {
    readHead :: HNil
  }.asInstanceOf[U]
}

// This has to be a regular class to be extended by the FileReader.
//   Later on, untangle to allow case class here
class ReaderV2[T <: Product](
  val fromSource: DataSource
)(
  implicit val sparkSession: SparkSession,
  implicit val readTypeTag: TypeTag[T]
) extends ReaderTrait {
  override type ReadAs = Dataset[T]

  implicit val readEncoder: Encoder[T] = Encoders.product[T]

  override def fileReader(file: FILE): GenericFileReader = FileReader[T](file)
}

private case class FileReader[T <: Product](
  file: FILE
)(
  implicit sparkSession: SparkSession, readTypeTag: TypeTag[T], readEncoder: Encoder[T]
) extends GenericFileReader(file) {
  override type ReadAs = Dataset[T]

  override def readHead: ReadAs = super.readHead.as[T]

  override def fileReader(file: FILE): GenericFileReader = this

  override lazy val schema: Option[StructType] = Some(readEncoder.schema)
}