package pfvalter.sparkles.core.framework.read.generic

import pfvalter.sparkles.core.io.source.{DataSource, FILE}
import shapeless.HList

/**
 * This Trait is the definition of a Reader (the actual interface implementation)
 *   It is extended by the (typed)Reader and the UntypedReader
 * @tparam R it's the actual type of what should be read (i.e. Dataset[T] or Dataframe)
 */
trait ReaderWithSource[R] extends ReaderBlueprint[R] {
  val fromSource: DataSource

  def fileReader(file: FILE): GenericFileReader[R]

  override def readHead: R = fromSource match {
    case file: FILE => fileReader(file).readHead
  }

  override def read[I <: HList]: () => I = fromSource match {
    case file: FILE => fileReader(file).read
  }
}
