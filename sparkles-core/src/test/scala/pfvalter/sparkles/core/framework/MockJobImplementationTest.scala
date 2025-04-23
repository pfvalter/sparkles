package pfvalter.sparkles.core.framework

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pfvalter.sparkles.core.framework.file.writer.MockOutputToFileWriter
import pfvalter.sparkles.core.framework.schemas.MockOutput
import pfvalter.sparkles.core.io.format._
import pfvalter.sparkles.core.framework.schemas.MockInput
import pfvalter.sparkles.core.io.read.SingleReaderFromFile

class MockJobImplementationTest extends AnyFlatSpec with Matchers {

  implicit val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate().newSession()

  "Mock" should "run" in {
    val reader = SingleReaderFromFile[MockInput](
      filePath = "test-files/json/input.json",
      fileFormat = JSON
    )
    val writer = MockOutputToFileWriter(
      filePath = "temp",
      fileFormat = PARQUET
    )

    val job: MockJobImplementation = MockJobImplementation(reader, writer)

    val results: Array[MockOutput] = job.apply().collect()

    results.length shouldBe 2
  }

}
