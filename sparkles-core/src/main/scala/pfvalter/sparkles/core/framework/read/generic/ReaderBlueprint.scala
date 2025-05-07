package pfvalter.sparkles.core.framework.read.generic

/**
 * This Trait serves as a blueprint for all Reader Implementations
 *
 * @tparam R it's the actual type of what should be read (i.e. Dataset[T] or Dataframe)
 */
trait ReaderBlueprint[R] extends Read {
  def readHead: R
}