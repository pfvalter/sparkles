package pfvalter.sparkles.core.framework

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pfvalter.sparkles.core.framework.file.writer.MockOutputToFileWriter
import pfvalter.sparkles.core.framework.schemas.{MockInput, MockInput2, MockOutput}
import pfvalter.sparkles.core.io.format._
import pfvalter.sparkles.core.io.read.{MultiReaderFromFiles, SingleReaderFromFile}

class MockMultiJobImplementationTest extends AnyFlatSpec with Matchers {

  implicit val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate().newSession()

  "MockMultiJob" should "run with multiple inputs" in {
    val multiReader = MultiReaderFromFiles(
      readers = Seq(
        SingleReaderFromFile[MockInput](
          FileMetadata(
            filePath = "test-files/json/input1/input.json",
            fileFormat = JSON
          )
        ),
        SingleReaderFromFile[MockInput2](
          FileMetadata(
            filePath = "test-files/json/input2/input.json",
            fileFormat = JSON
          )
        )
      )
    )

    val writer = MockOutputToFileWriter(
      filePath = "temp",
      fileFormat = PARQUET
    )

    val job: MockMultiJobImplementation = MockMultiJobImplementation(multiReader, writer)

    val results: Array[MockOutput] = job.apply().collect()

    results.length shouldBe 2
  }
} 