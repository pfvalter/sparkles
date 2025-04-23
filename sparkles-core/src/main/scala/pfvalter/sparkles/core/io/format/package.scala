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

  /**
   * -- fileFormat: FileFormat --
   * File Format of the file.
   * Note: Currently has no default. Later, a file type inference will fix that.
   */
  //Implement readFrom later, now let's just use Paths as Strings
  // -> val readFrom: FileLocation
  //This val is only here temporarily to hold the filePath
  case class FileMetadata(
    filePath: String,
    fileFormat: FileFormat
  )
}
