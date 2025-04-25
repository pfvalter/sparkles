package pfvalter.sparkles.core.io

import pfvalter.sparkles.core.io.format.FileFormat

package object source {

  trait DataSource

  case class FILE(
    filePath: String,
    fileFormat: FileFormat
  ) extends DataSource

  case object S3 extends DataSource
  case object SNOWFLAKE extends DataSource
}
