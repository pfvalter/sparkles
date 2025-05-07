package pfvalter.sparkles.core.framework.write.generic

import org.apache.spark.sql._
import pfvalter.sparkles.core.framework.write.Write
import pfvalter.sparkles.core.io.format.{CSV, JSON, PARQUET}
import pfvalter.sparkles.core.io.source.{DataSource, FILE}
import shapeless._

import scala.reflect.runtime.universe.TypeTag

trait WriterTrait extends Write {
  val toSource: DataSource

  implicit val sparkSession: SparkSession

  type OutputType

  type WriteAs

  def fileWriter(file: FILE, saveMode: SaveMode = SaveMode.Overwrite): GenericFileWriter

  def writeHead(output: WriteAs): WriteAs = toSource match {
    case file: FILE => {
      val writer: GenericFileWriter = fileWriter(file)

      writer.writeHead(output.asInstanceOf[writer.WriteAs]).asInstanceOf[WriteAs]
    }
  }

  def write[U <: HList](output: U): U = toSource match {
    case file: FILE => fileWriter(file).write(output)
  }
}

abstract class GenericFileWriter(
  file: FILE,
  //Implement this later to allow different SaveMode's
  saveMode: SaveMode = SaveMode.Overwrite
)(
  override implicit val sparkSession: SparkSession
) extends WriterTrait {
  override lazy val toSource: DataSource = file

  private def writeDS(output: WriteAs): Unit = output match {
    case ds: Dataset[OutputType] => file.fileFormat match {
      case JSON => ds.write.mode(saveMode).format("json").save(file.filePath)
      case PARQUET => ds.write.mode(saveMode).format("parquet").save(file.filePath)
      case CSV => ds.write.mode(saveMode).format("csv").save(file.filePath)
      //case TEXT => output.write.mode(saveMode).format("text").save(filePath)
      case _ => throw new Exception("File Format not defined")
    }
    case _ => throw new Exception("Unsupported Dataset")
  }

  override def writeHead(output: super.WriteAs): super.WriteAs = {
    writeDS(output)
    // Return back the output:
    output
  }

  override def write[U <: HList](output: U): U = {
    output match {
      case head :: HNil => writeHead(head.asInstanceOf[super.WriteAs])
    }
    // Return back the output:
    output
  }
}

// This has to be a regular class to be extended by the FileReader.
//   Later on, untangle to allow case class here
class WriterV2[T <: Product](
  val toSource: DataSource
)(
  implicit val sparkSession: SparkSession,
  implicit val readTypeTag: TypeTag[T]
) extends WriterTrait {

  override type OutputType = T

  override type WriteAs = Dataset[T]

  implicit val writeEncoder: Encoder[T] = Encoders.product[T]

  override def fileWriter(file: FILE, saveMode: SaveMode = SaveMode.Overwrite): GenericFileWriter = {
    FileWriter[T](file, saveMode)
  }
}

private case class FileWriter[T <: Product](
  file: FILE,
  saveMode: SaveMode
)(
  implicit spark: SparkSession, writeTypeTag: TypeTag[T]
) extends GenericFileWriter(file) {

  override def fileWriter(file: FILE, saveMode: SaveMode): GenericFileWriter = this
}