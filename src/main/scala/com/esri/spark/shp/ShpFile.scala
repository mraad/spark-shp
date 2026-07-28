package com.esri.spark.shp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}

import java.nio.{ByteBuffer, ByteOrder}

/**
 * ShpFile instance.
 *
 * @param stream the input stream, positioned at the first record.
 */
class ShpFile(stream: FSDataInputStream) extends AutoCloseable {

  var rowNum = 0

  private val header = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN)
  private var content = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN)

  /**
   * Read the next geometry.
   *
   * Note, the returned buffer is reused between calls. Copy the content if it has to outlive the call.
   *
   * @return the geometry in Esri shape format, positioned at 0 and limited to the record content length.
   */
  def next(): ByteBuffer = {
    header.rewind
    stream.readFully(header.array)
    rowNum = header.getInt
    val contentLen = header.getInt * 2
    if (contentLen > content.capacity) {
      content = ByteBuffer.allocate(contentLen).order(ByteOrder.LITTLE_ENDIAN)
    }
    stream.readFully(content.array, 0, contentLen)
    content.position(0)
    content.limit(contentLen)
    content
  }

  /**
   * Close the stream.
   */
  override def close(): Unit = {
    stream.close()
  }

}

/**
 * Supporting class object.
 */
object ShpFile extends Serializable {
  /**
   * Create ShpFile instance.
   *
   * @param pathName      the shape file path with or without the .shp extension.
   * @param configuration Hadoop configuration instance.
   * @return a ShpFile instance.
   */
  def apply(pathName: String, configuration: Configuration): ShpFile = {
    val path = new Path(pathName.stripSuffix(".shp") + ".shp")
    val stream = path.getFileSystem(configuration).open(path)
    // Validates the file signature and leaves the stream positioned at the first record.
    ShpHeader(stream)
    new ShpFile(stream)
  }
}
