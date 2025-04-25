package pfvalter.sparkles.core.framework

import shapeless.HList
import shapeless.ops.hlist.IsHCons

/**
 * "Job" is the main trait of the framework. It is the skeleton of a Spark Job written in Sparkles
 *   All it "knows" is:
 *   - What it will do to read (aka. what reader it should use)
 *   - What it will do to write (aka. what writer it should use)
 *   - What is the logic it should run
 *
 *   Then, there is an apply() method that just does the orchestration of:
 *   -> read data -> run code -> write data
 *
 * Implementations of this trait only need to inject a Reader and a Writer, declare its types,
 *   and then implement "run" with the real business logic
 */
trait Job  {
  val reader: Read
  val writer: Writer

  /*
   * This is the method that needs to be implemented:
   */
  def run[R <: HList](dataInput: R)(implicit ev: IsHCons[R]): writer.WriteType

  /*
   * Although you can re-implement this method, you shouldn't.
   *   It is just a "trigger" for read, run, write.
   */
  def apply[R <: HList]()(implicit ev: IsHCons[R]): writer.WriteType = {
    writer.write.apply(run(reader.read.apply())(ev))
  }
}
