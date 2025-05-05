package pfvalter.sparkles.core.framework.read

import shapeless.HList

/*
 * Basic abstract ReadType "carrier" trait
 */
trait Read {
  type InputType

  def read[U <: HList]: () => U
}

