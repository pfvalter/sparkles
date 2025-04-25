package pfvalter.sparkles.core.framework

import org.apache.spark.sql.Dataset
import pfvalter.sparkles.core.framework.schemas.{MockInput, MockOutput}
import pfvalter.sparkles.core.io.write.SingleDatasetWriter
import shapeless._
import shapeless.ops.hlist.IsHCons

case class MockJobImplementation(
                                  reader: Reader[MockInput],
                                  writer: SingleDatasetWriter[MockOutput]
) extends Job {

  override def run[L <: HList](in: L)(implicit ev: IsHCons[L]): Dataset[MockOutput] = {
    val mockInput: Dataset[MockInput] = in.head(ev).asInstanceOf[Dataset[MockInput]]

    mockInput.map{ input: MockInput =>
      MockOutput(
        fieldA = input.id,
        fieldB = input.field1,
        fieldC = if (input.id < 10) {
          Some(input.field1)
        } else {
          None
        }
      )
    }(writer.writeEncoder)
  }
}

