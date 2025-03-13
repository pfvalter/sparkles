package pfvalter.sparkles.core.io.write

import org.apache.spark.sql.{Dataset, SaveMode}
import pfvalter.sparkles.core.io.format._

trait SingleDatasetWriterToFile[T <: Product] extends SingleDatasetWriter[T] {
  /**
   * File Format of the file.
   * Note: Currently has no default. Later, a file type inference will fix that.
   */
  val fileFormat: FileFormat

  //Implement this later, now let's just use Paths as Strings
  //val writeTo: FileLocation
  //This val is only here temporarily to hold the filePath
  val filePath: String

  //Implement this later to allow different SaveMode's
  val saveMode: SaveMode = SaveMode.Overwrite

  override val write: Dataset[T] => Dataset[T] = { output: Dataset[T]  =>
    {
      // Write the file:
      {
        fileFormat match {
          case JSON => output.write.mode(saveMode).format("json").save(filePath)
          case PARQUET => output.write.mode(saveMode).format("parquet").save(filePath)
          case CSV => output.write.mode(saveMode).format("csv").save(filePath)
          //case TEXT => output.write.mode(saveMode).format("text").save(filePath)
          case _ => throw new Exception("File Format not defined")
        }
      }
      // Return the input back so that it can be used in tests or in chained in-memory jobs.
      output
    }
  }
}
