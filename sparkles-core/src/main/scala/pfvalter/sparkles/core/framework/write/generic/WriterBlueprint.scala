package pfvalter.sparkles.core.framework.write.generic

/**
 * This Trait serves as a blueprint for all Writer Implementations
 *
 * @tparam W it's the actual type of what should be written (i.e. Dataset[T] or Dataframe)
 */
trait WriterBlueprint[W] extends Write {
  /**
   * This method is similar to the "write" in "Write" but defines the capability to write only the first element
   *   of the Output Hlist.
   * @return a Dataset[T] or a Dataframe, when implemented.
   */
  def writeHead(output: W): W
}