package pfvalter.sparkles.core.framework.read.generic

import pfvalter.sparkles.core.io.source.{DataSource, FILE}
import shapeless.HList

/**
 * This Trait is the definition of a Reader (the actual interface implementation)
 *   It is extended by the TypedReader and the UntypedReader
 * @tparam R it's the actual type of what should be read (i.e. Dataset[T] or Dataframe)
 */
trait Reader[R] extends ReaderBlueprint[R] {
  /**
   * This variable specifies where the Data is to be read from.
   */
  val fromSource: DataSource

  /**
   * This method specifies, when implemented, what implementation of the FileReader is to be used.
   * @param file the File Metadata that specifies the file path and it's format.
   * @return the FileReader implementation.
   */
  def fileReader(file: FILE): GenericFileReader[R]

  /**
   * Implementation of the Reader: depending on the DataSource will call the corresponding readHead
   *  @return a Dataset[T] or a Dataframe, when implemented.
   */
  override def readHead: R = fromSource match {
    case file: FILE => fileReader(file).readHead
  }

  /**
   * Implementation of the Reader: depending on the DataSource will call the corresponding read
   *  @return the HList of all inputs, when implemented.
   */
  override def read[I <: HList]: () => I = fromSource match {
    case file: FILE => fileReader(file).read
  }
}
