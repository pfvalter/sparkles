package pfvalter.sparkles.core.framework

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pfvalter.sparkles.core.framework.read.Reader
import pfvalter.sparkles.core.framework.schemas.MockOutput
import pfvalter.sparkles.core.io.format._
import pfvalter.sparkles.core.framework.schemas.MockInput
import pfvalter.sparkles.core.framework.write.generic.WriterV2
import pfvalter.sparkles.core.io.source.FILE
import shapeless._

class MockJobImplementationTest extends AnyFlatSpec with Matchers {

  implicit val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate().newSession()

  "Mock" should "run" in {
    val reader = new Reader[MockInput](
      FILE(
        filePath = "test-files/json/input1/input.json",
        fileFormat = JSON
      )
    ) :: HNil

    val writer = new WriterV2[MockOutput](
      FILE(
        filePath = "temp",
        fileFormat = PARQUET
      )
    ) :: HNil

    val job: MockJobImplementation = MockJobImplementation(reader, writer)

    val output1 :: _ = job.apply()
    val results: Array[MockOutput] = output1.collect()

    results.length shouldBe 2
  }
}
