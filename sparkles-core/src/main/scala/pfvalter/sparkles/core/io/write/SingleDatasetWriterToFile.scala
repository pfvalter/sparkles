package pfvalter.sparkles.core.io.write

import org.apache.spark.sql.{Dataset, SaveMode}
import pfvalter.sparkles.core.io.write.SingleDatasetWriter

trait SingleDatasetWriterToFile[T <: Product] extends SingleDatasetWriter[T] {
  //Implement this later, now let's just use JSON files and writes
  //val fileFormat: FileFormat

  //Implement this later, now let's just use Paths as Strings
  //val writeTo: FileLocation
  //This val is only here temporarily to hold the filePath
  val filePath: String

  //Implement this later to allow different SaveMode's
  val saveMode: SaveMode = SaveMode.Overwrite

  override val write: Dataset[T] => Dataset[T] = { output: Dataset[T]  =>
    {
      output.write.mode(saveMode).json(filePath)
      output
    }
  }
}
