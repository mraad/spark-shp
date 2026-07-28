package com.esri.spark.shp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.slf4j.LoggerFactory

import java.nio.charset.{Charset, StandardCharsets}
import java.nio.{ByteBuffer, ByteOrder}

/**
 * Create a DBF File.
 *
 * @param header DBFHeader instance.
 * @param fields array of DBFField instances.
 * @param stream the input stream.
 */
class DBFFile(val header: DBFHeader,
              val fields: Array[DBFField],
              stream: FSDataInputStream
             ) extends AutoCloseable {

  private val buffer = ByteBuffer.allocate(header.rowLength).order(ByteOrder.BIG_ENDIAN)

  /**
   * Read the next row of attributes into the given array.
   *
   * @param row  the destination array.
   * @param from the index in the destination array to write the first field to.
   */
  def next(row: Array[Any], from: Int): Unit = {
    stream.readFully(buffer.array)
    var i = 0
    while (i < fields.length) {
      row(from + i) = fields(i).readValue(buffer)
      i += 1
    }
  }

  /**
   * Close the input stream.
   */
  override def close(): Unit = {
    stream.close()
  }
}

/**
 * Support class object.
 */
object DBFFile extends Serializable {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  /**
   * The shapefile default text encoding, used when there is no .cpg side file.
   */
  val DEFAULT_CHARSET: String = StandardCharsets.ISO_8859_1.name

  /**
   * Create DBFFile instance.
   *
   * @param pathName the base path to dbf file, with or without the .shp extension.
   * @param conf     Hadoop configuration reference.
   * @param columns  Columns to read, empty for all of them.
   * @return DBFFile instance.
   */
  def apply(pathName: String, conf: Configuration, columns: Array[String]): DBFFile = {
    apply(new Path(pathName.stripSuffix(".shp") + ".dbf"), conf, columns)
  }

  /**
   * Create DBFFile instance.
   *
   * @param path    Path instance to the dbf file.
   * @param conf    Hadoop configuration reference.
   * @param columns Columns to read, empty for all of them.
   * @return DBFFile instance.
   */
  def apply(path: Path, conf: Configuration, columns: Array[String]): DBFFile = {
    val fs = path.getFileSystem(conf)
    val charsetName = readCharsetName(fs, path)
    val stream = fs.open(path)
    val header = DBFHeader(stream)
    val fields = new Array[DBFField](header.numFields)
    // The first byte of a row is the record deletion flag.
    var offset = 1
    for (i <- fields.indices) {
      val field = DBFField(stream, offset, charsetName)
      fields(i) = field
      offset += field.length
    }
    val newFields = columns match {
      case Array() => fields
      case _ => fields.filter(field => columns.contains(field.name))
    }
    stream.seek(header.headerLength)
    new DBFFile(header, newFields, stream)
  }

  /**
   * Read the text encoding of a dbf file from its .cpg side file.
   *
   * @param fs      the file system holding the dbf file.
   * @param dbfPath the path to the dbf file.
   * @return the charset name, DEFAULT_CHARSET when there is no readable and supported .cpg.
   */
  private def readCharsetName(fs: FileSystem, dbfPath: Path): String = {
    val path = new Path(dbfPath.toString.stripSuffix(".dbf") + ".cpg")
    try {
      if (fs.exists(path)) {
        val len = fs.getFileStatus(path).getLen.toInt
        val bytes = new Array[Byte](len)
        using(fs.open(path))(_.readFully(bytes))
        val name = new String(bytes, StandardCharsets.US_ASCII).trim
        if (Charset.isSupported(name)) {
          return name
        }
        // ponytail: codepage aliases like "ANSI 1252" are not charset names. Map them here if they show up.
        logger.warn(s"$path specifies the unsupported charset '$name'. Falling back to $DEFAULT_CHARSET.")
      }
    } catch {
      case t: Throwable => logger.warn(s"Cannot read $path. ${t.toString}")
    }
    DEFAULT_CHARSET
  }
}
