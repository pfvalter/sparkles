package pfvalter.sparkles.core.framework.read

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import pfvalter.sparkles.core.framework.read.generic.{GenericFileReader, Reader}
import pfvalter.sparkles.core.io.source.{DataSource, FILE}

// This code is praticaly repeated. Explore the possibility of abstracting it later.
// This has to be a regular class to be extended by the FileReader.
//   Later on, untangle to allow case class here
class UntypedReader(
  val fromSource: DataSource
)(
  implicit val sparkSession: SparkSession
) extends Reader[DataFrame] {
  override def fileReader(file: FILE): GenericFileReader[DataFrame] = UntypedFileReader(file)
}

private case class UntypedFileReader(
  file: FILE
)(
  implicit sparkSession: SparkSession,
) extends GenericFileReader[DataFrame](file) {
  override def readHead: DataFrame = readDf

  override val schema: Option[StructType] = None
}