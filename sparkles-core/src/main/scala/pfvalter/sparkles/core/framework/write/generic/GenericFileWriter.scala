package pfvalter.sparkles.core.framework.write.generic

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import pfvalter.sparkles.core.io.format.{CSV, JSON, PARQUET}
import pfvalter.sparkles.core.io.source.FILE
import shapeless.{::, HList, HNil}

/**
 * Abstract class that provides reusable methods to both implementation (typed and untyped) of file readers
 *
 * @param file the File Metadata necessary to create a Reader
 * @param sparkSession just like the name says...
 * @tparam R it's the actual type of what should be read (i.e. Dataset[T] or Dataframe)
 */
abstract class GenericFileWriter[R](
  file: FILE,
  //Implement this later to allow different SaveMode's
  saveMode: SaveMode = SaveMode.Overwrite
)(
  implicit val sparkSession: SparkSession
) extends WriterBlueprint[R] {

  private def writeDS(output: R): Unit = output match {
    case ds: Dataset[_] => file.fileFormat match {
      case JSON => ds.write.mode(saveMode).format("json").save(file.filePath)
      case PARQUET => ds.write.mode(saveMode).format("parquet").save(file.filePath)
      case CSV => ds.write.mode(saveMode).format("csv").save(file.filePath)
      //case TEXT => output.write.mode(saveMode).format("text").save(filePath)
      case _ => throw new Exception("File Format not defined")
    }
    case _ => throw new Exception("Unsupported Dataset")
  }

  override def writeHead(output: R): R = {
    writeDS(output)
    // Return back the output:
    output
  }

  override def write[U <: HList](output: U): U = {
    output match {
      case (head: R) :: HNil => writeHead(head)
    }
    // Return back the output:
    output
  }
}
