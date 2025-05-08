package pfvalter.sparkles.core.framework.read.generic

/**
 * This Trait serves as a blueprint for all Reader Implementations
 *
 * @tparam R it's the actual type of what should be read (i.e. Dataset[T] or Dataframe)
 */
trait ReaderBlueprint[R] extends Read {
  /**
   * This method is similar to the "read" in "Read" but defines the capability to read only the first element of
   *   the Input Hlist.
   * @return a Dataset[T] or a Dataframe, when implemented.
   */
  def readHead: R
}