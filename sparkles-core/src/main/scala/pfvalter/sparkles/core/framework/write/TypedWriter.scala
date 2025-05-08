package pfvalter.sparkles.core.framework.write

import org.apache.spark.sql._
import pfvalter.sparkles.core.framework.write.generic.{GenericFileWriter, Writer}
import pfvalter.sparkles.core.io.source.{DataSource, FILE}

import scala.reflect.runtime.universe.TypeTag

/**
 * TypedWriter implementation
 * @param toSource the DataSource that specifies where to write the Data to.
 * @param sparkSession it's in the name...
 * @param writeTypeTag needed for Encoders
 * @tparam T the case class of the type of the Data to be written.
 */
class TypedWriter[T <: Product](
  val toSource: DataSource,
  val saveMode: SaveMode = SaveMode.Overwrite
)(
  implicit val sparkSession: SparkSession,
  implicit val writeTypeTag: TypeTag[T]
) extends Writer[Dataset[T]] {

  implicit val writeEncoder: Encoder[T] = Encoders.product[T]

  override def fileWriter(file: FILE, saveMode: SaveMode = saveMode): GenericFileWriter[Dataset[T]] = {
    TypedFileWriter[T](file, saveMode)
  }
}

/**
 * TypedFileWriter implementation
 * @param file the File Metadata necessary to create a Reader
 * @param saveMode the spark instruction Save Mode.
 * @param sparkSession just like the name says...
 * @param writeTypeTag needed for Encoders
 * @tparam T the case class of the type of the Data to be written.
 */
case class TypedFileWriter[T <: Product](
  file: FILE,
  saveMode: SaveMode
)(
  implicit sparkSession: SparkSession, writeTypeTag: TypeTag[T]
) extends GenericFileWriter[Dataset[T]](file, saveMode)