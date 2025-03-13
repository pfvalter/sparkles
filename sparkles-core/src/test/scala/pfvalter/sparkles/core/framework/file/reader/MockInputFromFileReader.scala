package pfvalter.sparkles.core.framework.file.reader

import org.apache.spark.sql.SparkSession
import pfvalter.sparkles.core.framework.schemas.MockInput
import pfvalter.sparkles.core.io.format.FileFormat
import pfvalter.sparkles.core.io.read.SingleDatasetReaderFromFile

import scala.reflect.runtime.universe.TypeTag

case class MockInputFromFileReader(
  filePath: String,
  fileFormat: FileFormat
)(
  implicit val spark: SparkSession,
  implicit val readTypeTag: TypeTag[MockInput]
) extends SingleDatasetReaderFromFile[MockInput]
