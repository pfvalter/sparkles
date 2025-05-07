package pfvalter.sparkles.core.framework

import org.apache.spark.sql.SparkSession
import pfvalter.sparkles.core.framework.read._
import pfvalter.sparkles.core.framework.read.generic.{MultiReader, Read}
import pfvalter.sparkles.core.framework.write._
import shapeless.HList

/**
 * "Job" is the main trait of the framework. It is the skeleton of a Spark Job written in Sparkles
 *   All it "knows" is:
 *   - What it will do to read (aka. what readers it should use)
 *   - What it will do to write (aka. what writers it should use)
 *   - What is the logic it should run
 *
 *   Then, there is an apply() method that just does the orchestration of:
 *   -> read data -> run code -> write data
 *
 * Implementations of this trait only need to inject the "readers" (HList) and "writers" (HList),
 *   declare the types of the input and output (i.e. Dataset[Something] or Dataframe, etc.),
 *   and then implement "run" with the real business logic
 */
trait Job[I <: HList, O <: HList]  {
  // Abstract the session part to be injected depending on the context (local, cluster, test, etc.)
  implicit val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate().newSession()

  val readers: HList
  val writers: HList
  private lazy val writer: Write = MultiWriter(writers)
  private lazy val reader: Read = MultiReader(readers)

  /*
   * This is the method that needs to be implemented:
   */
  def run(dataInput: I): O

  /*
   * Although you can re-implement this method, you shouldn't.
   *   It is just a "trigger" for read, run, write.
   */
  def apply(): O = {
    writer.write(run(reader.read.apply()))
  }
}
