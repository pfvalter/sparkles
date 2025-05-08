package pfvalter.sparkles.core.framework.write.generic

import org.apache.spark.sql.SaveMode
import pfvalter.sparkles.core.io.source.{DataSource, FILE}
import shapeless.HList

/**
 * This Trait is the definition of a Writer (the actual interface implementation)
 *   It is extended by the TypedWriter and the UntypedWriter
 *
 * @tparam W it's the actual type of what should be written (i.e. Dataset[T] or Dataframe)
 */
trait Writer[W] extends WriterBlueprint[W] {
  /*
   * This type is an artifact to be used for casting. If later there is a way to get rid of it, it will be deleted.
   */
  type WriteAs = W

  /**
   * This variable specifies where to write the Data to.
   */
  val toSource: DataSource

  /**
   * This method specifies, when implemented, what implementation of the FileWriter is to be used.
   * @param file the File Metadata that specifies the file path and it's format.
   * @return the FileWriter implementation.
   */
  def fileWriter(file: FILE, saveMode: SaveMode = SaveMode.Overwrite): GenericFileWriter[W]

  /**
   * Implementation of the Writer: depending on the DataSource will call the corresponding writeHead
   * @param output the output to be written.
   *  @return a Dataset[T] or a Dataframe, when implemented.
   */
  def writeHead(output: WriteAs): WriteAs = toSource match {
    case file: FILE => fileWriter(file).writeHead(output)
  }

  /**
   * Implementation of the Writer: depending on the DataSource will call the corresponding write
   * @param output the HList of all the outputs to be written.
   *  @return the HList of all outputs, when implemented.
   */
  def write[U <: HList](output: U): U = toSource match {
    case file: FILE => fileWriter(file).write(output)
  }
}