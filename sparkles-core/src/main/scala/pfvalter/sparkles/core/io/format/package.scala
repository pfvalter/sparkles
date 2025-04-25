package pfvalter.sparkles.core.io

/*
 * Package object that holds the enumerator of types used in the File Readers and Writers
 */
package object format {

  trait FileFormat

  case object JSON extends FileFormat
  case object PARQUET extends FileFormat
  case object CSV extends FileFormat
  case object TEXT extends FileFormat
}
