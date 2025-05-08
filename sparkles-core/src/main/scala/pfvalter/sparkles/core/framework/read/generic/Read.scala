package pfvalter.sparkles.core.framework.read.generic

import shapeless.HList

/*
 * Basic abstract Read trait.
 */
trait Read {
  /**
   * This method is implemented by all Readers and is the only method a Job will call inside it's "apply".
   * @tparam I The Input type, which is an HList with all the read Inputs chained.
   * @return an HList with all the read inputs.
   */
  def read[I <: HList]: () => I
}

