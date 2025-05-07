package pfvalter.sparkles.core.framework.read.generic

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import pfvalter.sparkles.core.io.format.{CSV, JSON, PARQUET, TEXT}
import pfvalter.sparkles.core.io.source.FILE
import shapeless.{HList, HNil}

/**
 * Abstract class that provides reusable methods to both implementation (typed and untyped) of file readers
 * @param file the File Metadata necessary to create a Reader
 * @param sparkSession just like the name says...
 * @tparam R it's the actual type of what should be read (i.e. Dataset[T] or Dataframe)
 */
abstract class GenericFileReader[R](
  file: FILE
)(
  implicit val sparkSession: SparkSession
) extends ReaderBlueprint[R] {
  val schema: Option[StructType]

  /**
   * Method that reads a Dataframe (with or without schema) allowing for both DF and DS reads to be done from it.
   * @return
   */
  def readDf: DataFrame = {
    val dfReader: DataFrameReader = schema match {
      case Some(schema) => sparkSession.read.schema(schema)
      case None         => sparkSession.read
    }

    file.fileFormat match {
      case JSON => dfReader.json(file.filePath)
      case PARQUET => dfReader.parquet(file.filePath)
      case CSV => dfReader.csv(file.filePath)
      case TEXT => dfReader.text(file.filePath)
      case _ => throw new Exception("File Format not defined")
    }
  }

  override def read[I <: HList]: () => I = () => {
    readHead :: HNil
  }.asInstanceOf[I]
}
