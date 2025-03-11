package pfvalter.sparkles.core.framework

/**
 * "Job" is the main trait of the framework. It is the skelleton of a Spark Job written in Sparkles
 *   All it "knows" is:
 *   - What it will do to read
 *   - What it will do to write
 *   - What it should run
 *
 *   Then, there is an apply() method that just does the orchestration of:
 *   -> read data -> run code -> write data
 *
 * Implementations of this trait only need to inject a Reader and a Writer, declare it's types,
 *   and then implement "run" with the real business logic
 */
trait Job extends Read with Write {
  val reader: Reader[ReadType]
  val writer: Writer[WriteType]

  def run(in: ReadType): WriteType

  def apply(): Boolean = writer.write(run(reader.read))
}
