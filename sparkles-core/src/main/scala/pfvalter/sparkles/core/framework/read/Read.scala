package pfvalter.sparkles.core.framework.read

import shapeless.HList

/*
 * Basic abstract ReadType "carrier" trait
 */
trait Read {
  def read[U <: HList]: () => U
}

