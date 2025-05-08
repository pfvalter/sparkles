package pfvalter.sparkles.core.framework.read

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import pfvalter.sparkles.core.framework.read.generic.{GenericFileReader, Reader}
import pfvalter.sparkles.core.io.source.{DataSource, FILE}

// This has to be a regular class to be extended by the FileReader.
//   Later on, untangle to allow case class here

/**
 * UntypedReader implementation
 * @param fromSource the DataSource that specifies where to read the Data from.
 * @param sparkSession just like the name says...
 */
class UntypedReader(
  val fromSource: DataSource
)(
  implicit val sparkSession: SparkSession
) extends Reader[DataFrame] {
  override def fileReader(file: FILE): GenericFileReader[DataFrame] = UntypedFileReader(file)
}

/**
 * UntypedFileReader implementation
 * @param file the File Metadata necessary to create a Reader
 * @param sparkSession just like the name says...
 */
private case class UntypedFileReader(
  file: FILE
)(
  implicit sparkSession: SparkSession,
) extends GenericFileReader[DataFrame](file) {
  override def readHead: DataFrame = readDf

  override val schema: Option[StructType] = None
}