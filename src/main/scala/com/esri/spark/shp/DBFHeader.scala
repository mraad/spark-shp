package com.esri.spark.shp

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.hadoop.fs.FSDataInputStream

/**
 * A DBF Header.
 *
 * @param numRows      the number of rows.
 * @param headerLength the length of the header.
 * @param rowLength    the length of the row.
 */
case class DBFHeader(numRows: Int, headerLength: Short, rowLength: Short) {

  /**
   * The number of fields.
   */
  val numFields: Int = (headerLength - 1) / 32 - 1

  /**
   * Converts given row number to a seek position in the stream.
   *
   * @param rowNum the record number.
   * @return the seek position in the stream
   */
  def rowNumToSeekPos(rowNum: Long): Long = {
    headerLength + rowLength * rowNum
  }
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
    val numRecords = buffer.getInt(4)
    val headerLen = buffer.getShort(8)
    val recordLen = buffer.getShort(10)
    new DBFHeader(numRecords, headerLen, recordLen)
  }

}
