package pfvalter.sparkles.core.framework

import org.apache.spark.sql.Dataset
import pfvalter.sparkles.core.io.read.SingleDatasetReader
import pfvalter.sparkles.core.io.write.SingleDatasetWriter

case class Input(
  field1: String,
  field2: Int
) extends Product

case class Output(
  fieldA: Int,
  fieldB: String,
  fieldC: Option[String]
) extends Product

case class MockJobImplementation(
  reader: SingleDatasetReader[Input],
  writer: SingleDatasetWriter[Output]
) extends Job {

  override def run(in: Dataset[Input]): Dataset[Output] = {
    in.map{ input: Input =>
      Output(
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

