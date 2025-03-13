package pfvalter.sparkles.core.framework

import org.apache.spark.sql.Dataset
import pfvalter.sparkles.core.framework.schemas.{MockInput, MockOutput}
import pfvalter.sparkles.core.io.read.SingleDatasetReader
import pfvalter.sparkles.core.io.write.SingleDatasetWriter

case class MockJobImplementation(
  reader: SingleDatasetReader[MockInput],
  writer: SingleDatasetWriter[MockOutput]
) extends Job {

  override def run(in: Dataset[MockInput]): Dataset[MockOutput] = {
    in.map{ input: MockInput =>
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

