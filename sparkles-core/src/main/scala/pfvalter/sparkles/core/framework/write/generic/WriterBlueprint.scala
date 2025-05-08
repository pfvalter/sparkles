package pfvalter.sparkles.core.framework.write.generic

/**
 * This Trait serves as a blueprint for all Writer Implementations
 *
 * @tparam W it's the actual type of what should be written (i.e. Dataset[T] or Dataframe)
 */
trait WriterBlueprint[W] extends Write {
  def writeHead(output: W): W
}