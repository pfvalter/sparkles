package pfvalter.sparkles.core.framework.write.generic

import shapeless.HList

/*
 * Basic abstract WriteType "carrier" trait
 */
trait Write {
  /**
   * This method is implemented by all Writers and is the only method a Job will call inside it's "apply".
   * @tparam O The Output type, which is an HList with all the read Outputs chained.
   * @return an HList with all the outputs that have been written.
   *
   *  NOTE: If you're asking yourself why the output is being passed back, it's because you might want to
   *        chain jobs in a pipeline and pass the output directly to the next step from memory directly.
   *        This also allows for tests to have access to the results withoug having to read from a folder/s3
   *        again.
   */
  def write[O <: HList](output: O): O
}


