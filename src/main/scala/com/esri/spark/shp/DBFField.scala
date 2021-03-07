package com.esri.spark.shp

import java.nio.{ByteBuffer, ByteOrder}
import java.sql.Date
import java.text.SimpleDateFormat
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}


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

/**
 * Inject debugger into readValue.
 */
trait DBFDebug extends Serializable {
  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  def readText(buffer: ByteBuffer, offset: Int, length: Int): String = {
    val text = new String(buffer.array(), offset, length).trim()
    if (logger.isDebugEnabled) {
      logger.debug(text)
    }
    text
  }
}

case class FieldDate(name: String, offset: Int, length: Int) extends DBFField with DBFDebug {
  // Date rather than Timestamp as DBF holds only YYYYMMDD !
  override type T = Date
  private val dateFormat = new SimpleDateFormat("yyyyMMdd")

  def toStructField(): StructField = {
    StructField(name, DateType, nullable = true)
  }

  def readValue(buffer: ByteBuffer): Date = {
    try {
      val date = dateFormat.parse(readText(buffer, offset, length))
      new Date(date.getTime)
    } catch {
      case t: Throwable =>
        logger.error(s"$name ${t.toString}")
        new Date(0L)
    }
  }
}

case class FieldString(name: String, offset: Int, length: Int) extends DBFField with DBFDebug {
  override type T = String

  def toStructField(): StructField = {
    StructField(name, StringType, nullable = true)
  }

  def readValue(buffer: ByteBuffer): String = {
    readText(buffer, offset, length)
  }
}

case class FieldShort(name: String, offset: Int, length: Int) extends DBFField with DBFDebug {
  override type T = Short

  def toStructField(): StructField = {
    StructField(name, ShortType, nullable = true)
  }

  override def readValue(buffer: ByteBuffer): Short = {
    try {
      readText(buffer, offset, length).toShort
    } catch {
      case t: Throwable =>
        logger.error(s"$name ${t.toString}")
        0
    }
  }
}

case class FieldInt(name: String, offset: Int, length: Int) extends DBFField with DBFDebug {
  override type T = Int

  def toStructField(): StructField = {
    StructField(name, IntegerType, nullable = true)
  }

  override def readValue(buffer: ByteBuffer): Int = {
    try {
      readText(buffer, offset, length).toInt
    } catch {
      case t: Throwable =>
        logger.error(s"$name ${t.toString}")
        0
    }
  }
}

case class FieldLong(name: String, offset: Int, length: Int) extends DBFField with DBFDebug {
  override type T = Long

  def toStructField(): StructField = {
    StructField(name, LongType, nullable = true)
  }

  override def readValue(buffer: ByteBuffer): Long = {
    try {
      readText(buffer, offset, length).toLong
    } catch {
      case t: Throwable =>
        logger.error(s"$name ${t.toString}")
        0L
    }
  }
}

case class FieldFloat(name: String, offset: Int, length: Int) extends DBFField with DBFDebug {
  override type T = Float

  def toStructField(): StructField = {
    StructField(name, FloatType, nullable = true)
  }

  override def readValue(buffer: ByteBuffer): Float = {
    try {
      readText(buffer, offset, length).toFloat
    } catch {
      case t: Throwable =>
        logger.error(s"$name ${t.toString}")
        0.0F
    }
  }
}

case class FieldDouble(name: String, offset: Int, length: Int) extends DBFField with DBFDebug {
  override type T = Double

  def toStructField(): StructField = {
    StructField(name, DoubleType, nullable = true)
  }

  override def readValue(buffer: ByteBuffer): Double = {
    try {
      readText(buffer, offset, length).toDouble
    } catch {
      case t: Throwable =>
        logger.error(s"$name ${t.toString}")
        0.0
    }
  }
}

case class FieldBoolean(name: String, offset: Int, length: Int) extends DBFField with DBFDebug {
  override type T = Boolean

  def toStructField(): StructField = {
    StructField(name, BooleanType, nullable = true)
  }

  override def readValue(buffer: ByteBuffer): Boolean = {
    try {
      readText(buffer, offset, length).toBoolean
    } catch {
      case t: Throwable =>
        logger.error(s"$name ${t.toString}")
        false
    }
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
