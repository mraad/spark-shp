package com.esri.spark.shp

import java.nio.{ByteBuffer, ByteOrder}
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/**
 * A DBF Field trait.
 */
trait DBFField extends Serializable {
  type T

  /**
   * @return the field name.
   */
  def name(): String

  /**
   * @return field offset in the the row.
   */
  def offset(): Int

  /**
   * @return the field length.
   */
  def length(): Int

  /**
   * @return SparkSQL field.
   */
  def toStructField(): StructField

  /**
   * Read the field value.
   *
   * @param buffer the stream byte buffer.
   * @return a field value of type T.
   */
  def readValue(buffer: ByteBuffer): T
}

case class FieldDate(name: String, offset: Int, length: Int) extends DBFField {
  // Date rather than Timestamp as DBF hold only YYYYMMDD !
  override type T = Date
  private val dateFormat = new SimpleDateFormat("yyyyMMdd")

  def toStructField(): StructField = {
    StructField(name, DateType)
  }

  def readValue(buffer: ByteBuffer): Date = {
    val text = new String(buffer.array(), offset, length).trim()
    val date = dateFormat.parse(text)
    new Date(date.getTime)
  }
}

case class FieldString(name: String, offset: Int, length: Int) extends DBFField {
  override type T = String

  def toStructField(): StructField = {
    StructField(name, StringType)
  }

  def readValue(buffer: ByteBuffer): String = {
    new String(buffer.array(), offset, length).trim()
  }
}

case class FieldShort(name: String, offset: Int, length: Int) extends DBFField {
  override type T = Short

  def toStructField(): StructField = {
    StructField(name, ShortType)
  }

  override def readValue(buffer: ByteBuffer): Short = {
    new String(buffer.array(), offset, length).trim().toShort
  }
}

case class FieldInt(name: String, offset: Int, length: Int) extends DBFField {
  override type T = Int

  def toStructField(): StructField = {
    StructField(name, IntegerType)
  }

  override def readValue(buffer: ByteBuffer): Int = {
    new String(buffer.array(), offset, length).trim().toInt
  }
}

case class FieldLong(name: String, offset: Int, length: Int) extends DBFField {
  override type T = Long

  def toStructField(): StructField = {
    StructField(name, LongType)
  }

  override def readValue(buffer: ByteBuffer): Long = {
    new String(buffer.array(), offset, length).trim().toLong
  }
}

case class FieldFloat(name: String, offset: Int, length: Int) extends DBFField {
  override type T = Float

  def toStructField(): StructField = {
    StructField(name, FloatType)
  }

  override def readValue(buffer: ByteBuffer): Float = {
    new String(buffer.array(), offset, length).trim().toFloat
  }
}

case class FieldDouble(name: String, offset: Int, length: Int) extends DBFField {
  override type T = Double

  def toStructField(): StructField = {
    StructField(name, DoubleType)
  }

  override def readValue(buffer: ByteBuffer): Double = {
    new String(buffer.array(), offset, length).trim().toDouble
  }
}

case class FieldBoolean(name: String, offset: Int, length: Int) extends DBFField {
  override type T = Boolean

  def toStructField(): StructField = {
    StructField(name, BooleanType)
  }

  override def readValue(buffer: ByteBuffer): Boolean = {
    new String(buffer.array(), offset, length).trim().toBoolean
  }
}

object DBFField extends Serializable {
  /**
   * Create a DBFField instance.
   *
   * @param stream the input stream.
   * @param offset the stream offset.
   * @return a DBFField instance.
   */
  def apply(stream: FSDataInputStream, offset: Int): DBFField = {

    val logger = LoggerFactory.getLogger(getClass)
    val buffer = ByteBuffer.allocate(32).order(ByteOrder.BIG_ENDIAN)
    stream.readFully(buffer.array)

    var nonZeroIndex = 10
    while (nonZeroIndex >= 0 && buffer.get(nonZeroIndex) == 0) {
      nonZeroIndex -= 1
    }
    val fieldName = new String(buffer.array, 0, nonZeroIndex + 1).toLowerCase
    val fieldType = buffer.get(11).toChar
    val fieldLength = buffer.get(16) & 0x00FF
    val decimalCount = buffer.get(17) & 0x00FF

    logger.debug(s"$fieldName $fieldType $fieldLength $decimalCount")

    fieldType match {
      case 'D' => FieldDate(fieldName, offset, fieldLength)
      case 'F' => if (fieldLength <= 13)
        FieldFloat(fieldName, offset, fieldLength)
      else
        FieldDouble(fieldName, offset, fieldLength)
      case 'L' => FieldBoolean(fieldName, offset, fieldLength)
      case 'N' => if (decimalCount > 0)
        FieldDouble(fieldName, offset, fieldLength)
      else if (fieldLength <= 5)
        FieldShort(fieldName, offset, fieldLength)
      else
        FieldLong(fieldName, offset, fieldLength)
      case _ => FieldString(fieldName, offset, fieldLength)
    }
  }
}
