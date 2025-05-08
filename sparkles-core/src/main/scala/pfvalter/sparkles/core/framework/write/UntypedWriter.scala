package pfvalter.sparkles.core.framework.write

import pfvalter.sparkles.core.framework.write.generic.{GenericFileWriter, Writer}
import pfvalter.sparkles.core.io.source.{DataSource, FILE}
import org.apache.spark.sql._

class UntypedWriter(
  val toSource: DataSource,
  //Implement this later to allow different SaveMode's
  val saveMode: SaveMode = SaveMode.Overwrite
)(
  implicit val sparkSession: SparkSession
) extends Writer[DataFrame] {
  override def fileWriter(file: FILE, saveMode: SaveMode): GenericFileWriter[DataFrame] = {
    UntypedFileWriter(file, saveMode)
  }
}

case class UntypedFileWriter(
  file: FILE,
  saveMode: SaveMode
)(
  implicit sparkSession: SparkSession
) extends GenericFileWriter[DataFrame](file, saveMode)