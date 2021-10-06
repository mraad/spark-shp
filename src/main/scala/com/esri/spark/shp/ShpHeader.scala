package com.esri.spark.shp

import org.slf4j.LoggerFactory

import java.io.{DataInputStream, IOException}
import java.nio.{ByteBuffer, ByteOrder}

/**
 * Shapefile header instance.
 */
class ShpHeader(val shapeType: Int,
                val xmin: Double,
                val ymin: Double,
                val xmax: Double,
                val ymax: Double,
                val zmin: Double,
                val zmax: Double,
                val mmin: Double,
                val mmax: Double
               ) extends Serializable

/**
 * Supporting class object.
 */
object ShpHeader extends Serializable {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Create ShpHeader instance.
   *
   * @param stream the input stream.
   * @return A ShpHeader instance.
   */
  def apply(stream: DataInputStream): ShpHeader = {
    val buffer = ByteBuffer.allocate(100).order(ByteOrder.BIG_ENDIAN)
    stream.readFully(buffer.array)

    val signature = buffer.getInt(0)
    if (signature != 9994) {
      throw new IOException("Not a valid shp or shx file. Expected 9994 as a file signature !")
    }

    buffer.order(ByteOrder.LITTLE_ENDIAN)
    val shapeType = buffer.getInt(32)
    val xmin = buffer.getDouble(36)
    val ymin = buffer.getDouble(44)
    val xmax = buffer.getDouble(52)
    val ymax = buffer.getDouble(60)
    val zmin = buffer.getDouble(68)
    val zmax = buffer.getDouble(76)
    val mmin = buffer.getDouble(84)
    val mmax = buffer.getDouble(92)
    if (logger.isDebugEnabled) {
      logger.debug(s"shapeType=$shapeType")
      logger.debug(s"xmin=$xmin ymin=$ymin")
      logger.debug(s"xmax=$xmax ymax=$ymax")
    }
    new ShpHeader(shapeType, xmin, ymin, xmax, ymax, zmin, zmax, mmin, mmax)
  }

}
