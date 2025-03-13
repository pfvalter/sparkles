package pfvalter.sparkles.core.framework

import org.apache.spark.sql.Dataset
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pfvalter.sparkles.core.framework.file.reader.MockInputFromFileReader
import pfvalter.sparkles.core.framework.file.writer.MockOutputToFileWriter
import pfvalter.sparkles.core.framework.schemas.MockOutput

class MockJobImplementationTest extends AnyFlatSpec with Matchers {

  "Mock" should "run" in {
    // Missing SparkContext here.
    val reader = MockInputFromFileReader("")
    val writer = MockOutputToFileWriter("")

    val job: MockJobImplementation = MockJobImplementation(reader, writer)

    val results: Dataset[MockOutput] = job.apply()

    results.collect().length shouldBe 2
  }

}
