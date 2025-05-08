package pfvalter.sparkles.core.framework.write

import org.apache.spark.sql._
import pfvalter.sparkles.core.framework.write.generic.{GenericFileWriter, Writer}
import pfvalter.sparkles.core.io.source.{DataSource, FILE}

import scala.reflect.runtime.universe.TypeTag

class TypedWriter[T <: Product](
  val toSource: DataSource,
  //Implement this later to allow different SaveMode's
  saveMode: SaveMode = SaveMode.Overwrite
)(
  implicit val spark: SparkSession,
  implicit val writeTypeTag: TypeTag[T]
) extends Writer[Dataset[T]] {

  implicit val writeEncoder: Encoder[T] = Encoders.product[T]

  override def fileWriter(file: FILE, saveMode: SaveMode): GenericFileWriter[Dataset[T]] = ???
}

class TypedReader[T <: Product](
  val toSource: DataSource,
  //Implement this later to allow different SaveMode's
  saveMode: SaveMode = SaveMode.Overwrite
)(
  implicit val sparkSession: SparkSession,
  implicit val readTypeTag: TypeTag[T]
) extends Writer[Dataset[T]] {

  implicit val readEncoder: Encoder[T] = Encoders.product[T]

  override def fileWriter(file: FILE, saveMode: SaveMode): GenericFileWriter[Dataset[T]] = {
    TypedFileWriter[T](file, saveMode)
  }
}

case class TypedFileWriter[T <: Product](
  file: FILE,
  saveMode: SaveMode
)(
  implicit sparkSession: SparkSession, writeTypeTag: TypeTag[T]
) extends GenericFileWriter[Dataset[T]](file, saveMode)