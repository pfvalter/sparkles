package pfvalter.sparkles.core.framework

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pfvalter.sparkles.core.framework.read.TypedReader
import pfvalter.sparkles.core.framework.schemas.{MockInput, MockInput2, MockOutput, MockOutput2}
import pfvalter.sparkles.core.framework.write.TypedWriter
import pfvalter.sparkles.core.io.format._
import pfvalter.sparkles.core.io.source.FILE
import shapeless._

class MockMultiJobImplementationTest extends AnyFlatSpec with Matchers {

  implicit val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate().newSession()

  "MockMultiJob" should "run with multiple inputs" in {
    val readers = new TypedReader[MockInput](
      FILE(
        filePath = "test-files/json/input1/input.json",
        fileFormat = JSON
      )
    ) :: new TypedReader[MockInput2](
      FILE(
        filePath = "test-files/json/input2/input.json",
        fileFormat = JSON
      )
    ) :: HNil

    val writers = new TypedWriter[MockOutput](
      FILE(
        filePath = "temp/output1",
        fileFormat = PARQUET
      )
    ) :: new TypedWriter[MockOutput2](
      FILE(
        filePath = "temp/output2",
        fileFormat = PARQUET
      )
    ) :: HNil

    val job: MockMultiJobImplementation = MockMultiJobImplementation(readers, writers)

    val output1 :: output2 :: _ = job.apply()
    val results: Array[MockOutput] = output1.collect()
    val results2: Array[MockOutput2] = output2.collect()

    results.length shouldBe 2
    results2.length shouldBe 2
  }
} 