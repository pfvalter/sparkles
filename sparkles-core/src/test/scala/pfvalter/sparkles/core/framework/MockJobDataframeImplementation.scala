package pfvalter.sparkles.core.framework

import org.apache.spark.sql.DataFrame
import pfvalter.sparkles.core.framework.read.UntypedReader
import pfvalter.sparkles.core.framework.write.UntypedWriter
import shapeless._

case class MockJobDataframeImplementation(
  readers: UntypedReader :: HNil,
  writers: UntypedWriter :: HNil
) extends Job[
  DataFrame :: HNil,
  DataFrame :: HNil
] {

  override def run(
    dataInput: DataFrame :: HNil
  ): DataFrame :: HNil = {
    import sparkSession.implicits._
    val mockInput: DataFrame = dataInput.head

    val schemaPrint: String = mockInput.schema.treeString

    val splitText: List[String] = schemaPrint.split("\n").toList

    splitText.toDF() :: HNil
  }
}

