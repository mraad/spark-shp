package com.esri.spark.shp

import java.nio.{ByteBuffer, ByteOrder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
// import org.slf4j.LoggerFactory

/**
 * ShpFile instance.
 *
 * @param shpHeader the shapefile header.
 * @param stream    the input stream.
 */
class ShpFile(shpHeader: ShpHeader,
              stream: FSDataInputStream
             ) extends Serializable with AutoCloseable {

  var rowNum = 0

  private val header = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN)
  // private val logger = LoggerFactory.getLogger(getClass)

  /**
   * @return geometry as an array of bytes.
   */
  def next(): Array[Byte] = {
    header.rewind
    stream.readFully(header.array)
    rowNum = header.getInt
    val contentLen = header.getInt * 2
    val contentArr = Array.ofDim[Byte](contentLen)
    //    if (logger.isDebugEnabled) {
    //      logger.debug(s"next::rowNum=$rowNum contentLen=$contentLen")
    //    }
    stream.readFully(contentArr, 0, contentLen)
    contentArr
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
   * @param pathName      the shape file path without .shp extension.
   * @param configuration Hadoop configuration instance.
   * @param seekPosition  the seek position in the input stream.
   * @return a ShpFile instance.
   */
  def apply(pathName: String, configuration: Configuration, seekPosition: Long): ShpFile = {
    val _pathName = if (pathName.endsWith(".shp")) pathName else pathName + ".shp"
    val path = new Path(_pathName)
    val stream = path.getFileSystem(configuration).open(path)
    val shpHeader = ShpHeader(stream)
    stream.seek(100L.max(seekPosition))
    new ShpFile(shpHeader, stream)
  }
}
