package pfvalter.sparkles.core.framework.write

import org.apache.spark.sql._
import pfvalter.sparkles.core.framework.write.generic.{GenericFileWriter, WriterTrait}
import pfvalter.sparkles.core.io.format.{CSV, JSON, PARQUET}
import pfvalter.sparkles.core.io.source.{DataSource, FILE}
import shapeless._

import scala.reflect.runtime.universe.TypeTag

class UntypedWriter(
  val toSource: DataSource,
  //Implement this later to allow different SaveMode's
  val saveMode: SaveMode = SaveMode.Overwrite
)(
  implicit val sparkSession: SparkSession
) extends WriterTrait {

  override type WriteAs = DataFrame

  override def fileWriter(file: FILE, saveMode: SaveMode = SaveMode.Overwrite): GenericFileWriter = {
    UntypedFileWriter(file, saveMode)
  }
}

case class UntypedFileWriter(
  file: FILE,
  saveMode: SaveMode
)(
  implicit sparkSession: SparkSession
) extends GenericFileWriter(file, saveMode) {

  override def fileWriter(file: FILE, saveMode: SaveMode = SaveMode.Overwrite): GenericFileWriter = this
}