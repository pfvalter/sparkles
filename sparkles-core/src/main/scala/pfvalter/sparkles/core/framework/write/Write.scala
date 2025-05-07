package pfvalter.sparkles.core.framework.write

import shapeless.HList

/*
 * Basic abstract WriteType "carrier" trait
 */
trait Write {
  def write[O <: HList](output: O): O
}


