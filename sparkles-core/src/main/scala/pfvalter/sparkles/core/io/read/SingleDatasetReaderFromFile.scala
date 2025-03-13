package pfvalter.sparkles.core.io.read

import org.apache.spark.sql.{Dataset, SparkSession}

trait SingleDatasetReaderFromFile[T <: Product] extends SingleDatasetReader[T] {
  //Implement this later, now let's just use JSON files and readers
  //val fileFormat: FileFormat

  //Implement this later, now let's just use Paths as Strings
  //val readFrom: FileLocation
  //This val is only here temporarily to hold the filePath
  val filePath: String

  implicit val spark: SparkSession

  override val read: () => Dataset[T] = () => spark.read.json(filePath).as[T]
}
