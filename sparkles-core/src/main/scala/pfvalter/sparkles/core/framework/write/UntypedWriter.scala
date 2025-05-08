package pfvalter.sparkles.core.framework.write

import pfvalter.sparkles.core.framework.write.generic.{GenericFileWriter, Writer}
import pfvalter.sparkles.core.io.source.{DataSource, FILE}
import org.apache.spark.sql._

/**
 * UntypedWriter implementation
 * @param toSource the DataSource that specifies where to write the Data to.
 * @param saveMode the spark instruction Save Mode.
 * @param sparkSession just like the name says...
 */
class UntypedWriter(
  val toSource: DataSource,
  val saveMode: SaveMode = SaveMode.Overwrite
)(
  implicit val sparkSession: SparkSession
) extends Writer[DataFrame] {
  override def fileWriter(file: FILE, saveMode: SaveMode = saveMode): GenericFileWriter[DataFrame] = {
    UntypedFileWriter(file, saveMode)
  }
}

/**
 * UntypedFileWriter implementation
 * @param file the File Metadata necessary to create a Reader
 * @param saveMode the spark instruction Save Mode.
 * @param sparkSession just like the name says...
 */
case class UntypedFileWriter(
  file: FILE,
  saveMode: SaveMode
)(
  implicit sparkSession: SparkSession
) extends GenericFileWriter[DataFrame](file, saveMode)