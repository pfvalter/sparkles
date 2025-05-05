package pfvalter.sparkles.core.framework.write

import org.apache.spark.sql._
import pfvalter.sparkles.core.io.format.{CSV, JSON, PARQUET}
import pfvalter.sparkles.core.io.source.{DataSource, FILE}
import shapeless._

import scala.reflect.runtime.universe.TypeTag

class Writer[T <: Product](
  toSource: DataSource,
  //Implement this later to allow different SaveMode's
  saveMode: SaveMode = SaveMode.Overwrite
)(
  implicit val spark: SparkSession,
  implicit val writeTypeTag: TypeTag[T]
) extends Write {
  type OutputType = T

  implicit val writeEncoder: Encoder[T] = Encoders.product[T]

  def writeHead(output: Dataset[OutputType]): Dataset[OutputType] = toSource match {
    case file: FILE => FileWriter[T](file, saveMode).writeHead(output)
  }

  override def write[U <: HList](output: U): U = toSource match {
    case file: FILE => FileWriter[T](file, saveMode).write(output)
  }
}

case class FileWriter[T <: Product](
  file: FILE,
  saveMode: SaveMode
)(
  implicit spark: SparkSession, writeTypeTag: TypeTag[T]
) extends Writer(file) {

  override def writeHead(output: Dataset[T]): Dataset[T] = {
    file.fileFormat match {
      case JSON => output.write.mode(saveMode).format("json").save(file.filePath)
      case PARQUET => output.write.mode(saveMode).format("parquet").save(file.filePath)
      case CSV => output.write.mode(saveMode).format("csv").save(file.filePath)
      //case TEXT => output.write.mode(saveMode).format("text").save(filePath)
      case _ => throw new Exception("File Format not defined")
    }
    // Return back the output:
    output
  }

  override def write[U <: HList](output: U): U = {
    output match {
      case head :: HNil => writeHead(head.asInstanceOf[Dataset[T]])
    }
    // Return back the output:
    output
  }
}