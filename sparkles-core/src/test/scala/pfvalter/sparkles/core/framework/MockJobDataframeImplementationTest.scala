package pfvalter.sparkles.core.framework

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pfvalter.sparkles.core.framework.read.UntypedReader
import pfvalter.sparkles.core.framework.write.UntypedWriter
import pfvalter.sparkles.core.io.format._
import pfvalter.sparkles.core.io.source.FILE
import shapeless._

class MockJobDataframeImplementationTest extends AnyFlatSpec with Matchers {

  implicit val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate().newSession()

  "Mock" should "run" in {
    val reader = new UntypedReader(
      FILE(
        filePath = "test-files/json/input1/input.json",
        fileFormat = JSON
      )
    ) :: HNil

    val writer = new UntypedWriter(
      FILE(
        filePath = "temp/schema",
        fileFormat = PARQUET
      )
    ) :: HNil

    val job: MockJobDataframeImplementation = MockJobDataframeImplementation(reader, writer)

    val output1 :: _ = job.apply()
    val results: Array[Row] = output1.collect()

    results.length shouldBe 3
  }
}
