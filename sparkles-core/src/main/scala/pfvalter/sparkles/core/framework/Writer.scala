package pfvalter.sparkles.core.framework

/*
 * Basic abstract Writer trait
 */
trait Writer[T] {
  def write(in: T): Boolean
}

/*
 * Basic abstract WriteType "carrier" trait
 */
trait Write {
  type WriteType
}