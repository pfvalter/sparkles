package pfvalter.sparkles.core.framework

import org.apache.spark.sql.Dataset
import pfvalter.sparkles.core.framework.schemas.{MockInput, MockOutput, MockInput2}
import pfvalter.sparkles.core.io.read.MultiReaderFromFiles
import pfvalter.sparkles.core.io.write.SingleDatasetWriter
import shapeless._
import shapeless.ops.hlist.IsHCons

case class MockMultiJobImplementation(
  reader: MultiReaderFromFiles,
  writer: SingleDatasetWriter[MockOutput]
) extends Job {

  override def run[L <: HList](in: L)(implicit ev: IsHCons[L]): Dataset[MockOutput] = {
    val input1 :: input2 :: _ = in
    val mockInput1 = input1.asInstanceOf[Dataset[MockInput]]
    val mockInput2 = input2.asInstanceOf[Dataset[MockInput2]]

    mockInput1.joinWith(mockInput2, mockInput1("id") === mockInput2("id"))
      .map { case (input1, input2) =>
        MockOutput(
          fieldA = input1.id,
          fieldB = input1.field1,
          fieldC = if (input1.id < 10) {
            Some(input2.field2.toString)
          } else {
            None
          }
        )
      }(writer.writeEncoder)
  }
} 