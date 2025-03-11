package pfvalter.sparkles.core.framework

/*
 * Basic abstract Reader trait
 */
trait Reader[T] {
  def read: T
}

/*
 * Basic abstract ReadType "carrier" trait
 */
trait Read {
  type ReadType
}