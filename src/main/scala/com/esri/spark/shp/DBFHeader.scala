package com.esri.spark.shp

import org.apache.hadoop.fs.FSDataInputStream

import java.nio.{ByteBuffer, ByteOrder}

/**
 * A DBF Header.
 *
 * @param numRows      the number of rows.
 * @param headerLength the length of the header.
 * @param rowLength    the length of the row.
 */
case class DBFHeader(numRows: Int, headerLength: Int, rowLength: Int) {

  /**
   * The number of fields.
   */
  val numFields: Int = (headerLength - 1) / 32 - 1
}

/**
 * Supporting class object.
 */
object DBFHeader extends Serializable {

  /**
   * Create DBFHeader instance.
   *
   * @param stream the input stream.
   * @return A DBFHeader instance.
   */
  def apply(stream: FSDataInputStream): DBFHeader = {
    val buffer = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN)
    stream.readFully(buffer.array)
    // The header and record lengths are _unsigned_ shorts, a wide dbf overflows a signed short.
    new DBFHeader(buffer.getInt(4), buffer.getShort(8) & 0xFFFF, buffer.getShort(10) & 0xFFFF)
  }

}
