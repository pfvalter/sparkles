package pfvalter.sparkles.core.framework

import org.apache.spark.sql.Dataset
import pfvalter.sparkles.core.framework.schemas.{MockInput, MockOutput}
import pfvalter.sparkles.core.framework.SingleReader
import pfvalter.sparkles.core.io.write.SingleDatasetWriter
import shapeless._
import shapeless.ops.hlist.IsHCons

case class MockJobImplementation(
  reader: SingleReader[MockInput],
  writer: SingleDatasetWriter[MockOutput]
) extends Job {

  override def run[L <: HList](in: L)(implicit ev: IsHCons[L]): Dataset[MockOutput] = {
    val mockInput: Dataset[MockInput] = in.head(ev).asInstanceOf[Dataset[MockInput]]

    mockInput.map{ input: MockInput =>
      MockOutput(
        fieldA = input.field2,
        fieldB = input.field1,
        fieldC = if (input.field2 < 10) {
          Some(input.field1)
        } else {
          None
        }
      )
    }(writer.writeEncoder)
  }
}

