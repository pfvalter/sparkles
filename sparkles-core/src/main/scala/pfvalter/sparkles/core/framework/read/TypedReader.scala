package pfvalter.sparkles.core.framework.read

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import pfvalter.sparkles.core.framework.read.generic.{GenericFileReader, Reader}
import pfvalter.sparkles.core.io.source.{DataSource, FILE}

import scala.reflect.runtime.universe.TypeTag

// This has to be a regular class to be extended by the TypedFileReader.
//   Later on, untangle to allow case class here
/**
 * TypedReader implementation
 * @param fromSource the DataSource that specifies where to read the Data from.
 * @param sparkSession it's in the name...
 * @param readTypeTag needed for Encoders
 * @tparam T the case class of the type of the Data to be read.
 */
class TypedReader[T <: Product](
  val fromSource: DataSource
)(
  implicit val sparkSession: SparkSession,
  implicit val readTypeTag: TypeTag[T]
) extends Reader[Dataset[T]] {

  implicit val readEncoder: Encoder[T] = Encoders.product[T]

  override def fileReader(file: FILE): GenericFileReader[Dataset[T]] = TypedFileReader[T](file)
}

private case class TypedFileReader[T <: Product](
  file: FILE
)(
  implicit sparkSession: SparkSession, readTypeTag: TypeTag[T], readEncoder: Encoder[T]
) extends GenericFileReader[Dataset[T]](file) {
  override def readHead: Dataset[T] = readDf.as[T]

  override val schema: Option[StructType] = Some(readEncoder.schema)
}