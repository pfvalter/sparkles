package pfvalter.sparkles.core.framework.file.writer

import pfvalter.sparkles.core.framework.schemas.MockOutput
import pfvalter.sparkles.core.io.format.FileFormat
import pfvalter.sparkles.core.io.write.SingleDatasetWriterToFile

import scala.reflect.runtime.universe.TypeTag

case class MockOutputToFileWriter(
  filePath: String,
  fileFormat: FileFormat
)(
  implicit val writeTypeTag: TypeTag[MockOutput]
) extends SingleDatasetWriterToFile[MockOutput]
