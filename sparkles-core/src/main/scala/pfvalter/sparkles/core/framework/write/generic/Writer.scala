package pfvalter.sparkles.core.framework.write.generic

import org.apache.spark.sql.SaveMode
import pfvalter.sparkles.core.io.source.{DataSource, FILE}
import shapeless.HList

/**
 * This Trait is the definition of a Writer (the actual interface implementation)
 *   It is extended by the TypedWriter and the UntypedWriter
 *
 * @tparam W it's the actual type of what should be written (i.e. Dataset[T] or Dataframe)
 */
trait Writer[W] extends WriterBlueprint[W] {
  type WriteAs = W

  val toSource: DataSource

  def fileWriter(file: FILE, saveMode: SaveMode = SaveMode.Overwrite): GenericFileWriter[W]

  def writeHead(output: WriteAs): WriteAs = toSource match {
    case file: FILE => fileWriter(file).writeHead(output)
  }

  def write[U <: HList](output: U): U = toSource match {
    case file: FILE => fileWriter(file).write(output)
  }
}