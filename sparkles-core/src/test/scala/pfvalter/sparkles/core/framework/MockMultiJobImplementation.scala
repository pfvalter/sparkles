package pfvalter.sparkles.core.framework

import org.apache.spark.sql.Dataset
import pfvalter.sparkles.core.framework.read.Reader
import pfvalter.sparkles.core.framework.schemas._
import pfvalter.sparkles.core.framework.write._
import shapeless._

case class MockMultiJobImplementation(
  readers: Reader[MockInput] :: Reader[MockInput2] :: HNil,
  writers: Writer[MockOutput] :: Writer[MockOutput2] :: HNil
) extends Job[
  Dataset[MockInput] :: Dataset[MockInput2] :: HNil,
  Dataset[MockOutput] :: Dataset[MockOutput2] :: HNil,
  Reader[MockInput] :: Reader[MockInput2] :: HNil,
  Writer[MockOutput] :: Writer[MockOutput2] :: HNil
] {

  override def run(
    dataInput: Dataset[MockInput] :: Dataset[MockInput2] :: HNil
  ): Dataset[MockOutput] :: Dataset[MockOutput2] :: HNil = {
    val mockInput1 :: mockInput2 :: _ = dataInput
    val writer1 :: writer2 :: _ = writers

    val result = mockInput1.joinWith(mockInput2, mockInput1("id") === mockInput2("id"))
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
      }(writer1.writeEncoder)

    val result2 = mockInput1.joinWith(mockInput2, mockInput1("id") === mockInput2("id"))
      .map { case (input1, input2) =>
        MockOutput2(
          fieldD = input1.id,
          fieldE = input2.field2,
          fieldF = if (input2.field2) {
            Some(input2.field2)
          } else {
            None
          }
        )
      }(writer2.writeEncoder)

    result :: result2 :: HNil
  }
}