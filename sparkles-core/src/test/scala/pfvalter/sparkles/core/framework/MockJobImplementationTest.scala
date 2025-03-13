package pfvalter.sparkles.core.framework

import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pfvalter.sparkles.core.framework.file.reader.MockInputFromFileReader
import pfvalter.sparkles.core.framework.file.writer.MockOutputToFileWriter
import pfvalter.sparkles.core.framework.schemas.MockOutput

class MockJobImplementationTest extends AnyFlatSpec with Matchers {

  implicit val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate().newSession()

  "Mock" should "run" in {
    val reader = MockInputFromFileReader("test-files/input.json")
    val writer = MockOutputToFileWriter("temp")

    val job: MockJobImplementation = MockJobImplementation(reader, writer)

    val results: Array[MockOutput] = job.apply().collect()

    results.length shouldBe 2
  }

}
