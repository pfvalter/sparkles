package pfvalter.sparkles.core.io.read

import org.apache.spark.sql.{Dataset, SparkSession}
import pfvalter.sparkles.core.io.format._

trait SingleDatasetReaderFromFile[T <: Product] extends SingleDatasetReader[T] {
  /**
   * File Format of the file.
   * Note: Currently has no default. Later, a file type inference will fix that.
   */
  val fileFormat: FileFormat

  //Implement this later, now let's just use Paths as Strings
  //val readFrom: FileLocation
  //This val is only here temporarily to hold the filePath
  val filePath: String

  implicit val spark: SparkSession

  override val read: () => Dataset[T] = () => {
    fileFormat match {
      case JSON => spark.read.json(filePath).as[T]
      case PARQUET => spark.read.parquet(filePath).as[T]
      case CSV => spark.read.csv(filePath).as[T]
      case TEXT => spark.read.text(filePath).as[T]
      case _ => throw new Exception("File Format not defined")
    }
  }
}
