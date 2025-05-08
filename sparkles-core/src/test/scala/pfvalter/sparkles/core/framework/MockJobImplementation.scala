package pfvalter.sparkles.core.framework

import org.apache.spark.sql.Dataset
import pfvalter.sparkles.core.framework.read.TypedReader
import pfvalter.sparkles.core.framework.schemas.{MockInput, MockOutput}
import pfvalter.sparkles.core.framework.write.TypedWriter
import shapeless._

case class MockJobImplementation(
  readers: TypedReader[MockInput] :: HNil,
  writers: TypedWriter[MockOutput] :: HNil
) extends Job[
  Dataset[MockInput] :: HNil,
  Dataset[MockOutput] :: HNil
] {

  override def run(
    dataInput: Dataset[MockInput] :: HNil
  ): Dataset[MockOutput] :: HNil = {
    val mockInput: Dataset[MockInput] = dataInput.head

    val result = mockInput.map{ input: MockInput =>
      MockOutput(
        fieldA = input.id,
        fieldB = input.field1,
        fieldC = if (input.id < 10) {
          Some(input.field1)
        } else {
          None
        }
      )
    }(writers.head.writeEncoder)

    result :: HNil
  }
}

