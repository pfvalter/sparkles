package pfvalter.sparkles.core.framework.read.generic

import shapeless.HList

/*
 * Basic abstract ReadType "carrier" trait
 */
trait Read {
  def read[I <: HList]: () => I
}

