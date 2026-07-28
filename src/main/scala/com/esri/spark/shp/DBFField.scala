package com.esri.spark.shp

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.{Charset, StandardCharsets}
import java.nio.{ByteBuffer, ByteOrder}
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Locale

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
 * Text reading and parsing support.
 */
trait DBFText extends Serializable {
  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
   * Read a blank trimmed text value, allocating exactly one String.
   *
   * @param buffer  the row byte buffer.
   * @param offset  the field offset in the row.
   * @param length  the field length.
   * @param charset the text encoding. Field values other than text are always ASCII.
   * @return the trimmed text.
   */
  @inline
  final def readText(buffer: ByteBuffer,
                     offset: Int,
                     length: Int,
                     charset: Charset = StandardCharsets.US_ASCII
                    ): String = {
    val arr = buffer.array()
    var beg = offset
    var end = offset + length
    // dbf pads with blanks, but nulls are found in the wild too.
    while (beg < end && (arr(beg) & 0xFF) <= ' ') beg += 1
    while (end > beg && (arr(end - 1) & 0xFF) <= ' ') end -= 1
    new String(arr, beg, end - beg, charset)
  }

  /**
   * Parse a field value, falling back to a default rather than failing the whole read.
   *
   * A blank value takes the default without throwing, as raising and logging an exception
   * per blank cell dominates the read time of a sparsely populated dbf.
   */
  @inline
  final def parse[V](buffer: ByteBuffer,
                     offset: Int,
                     length: Int,
                     name: String,
                     default: V
                    )(f: String => V): V = {
    val text = readText(buffer, offset, length)
    if (text.isEmpty) default
    else try {
      f(text)
    } catch {
      case t: Throwable =>
        logger.warn(s"$name ${t.toString}")
        default
    }
  }
}

case class FieldDate(name: String, offset: Int, length: Int) extends DBFField with DBFText {
  // Date rather than Timestamp as DBF holds only YYYYMMDD !
  override type T = Date
  private val dateFormat = new SimpleDateFormat("yyyyMMdd")

  def toStructField(): StructField = {
    StructField(name, DateType, nullable = true)
  }

  def readValue(buffer: ByteBuffer): Date = {
    parse(buffer, offset, length, name, null: Date) { text =>
      new Date(dateFormat.parse(text).getTime)
    }
  }
}

case class FieldString(name: String, offset: Int, length: Int, charsetName: String) extends DBFField with DBFText {
  override type T = String

  // Charset is not serializable, the name is.
  @transient private lazy val charset = Charset.forName(charsetName)

  def toStructField(): StructField = {
    StructField(name, StringType, nullable = true)
  }

  def readValue(buffer: ByteBuffer): String = {
    readText(buffer, offset, length, charset)
  }
}

case class FieldShort(name: String, offset: Int, length: Int) extends DBFField with DBFText {
  override type T = Short

  def toStructField(): StructField = {
    StructField(name, ShortType, nullable = true)
  }

  override def readValue(buffer: ByteBuffer): Short = {
    parse(buffer, offset, length, name, 0: Short)(_.toShort)
  }
}

case class FieldInt(name: String, offset: Int, length: Int) extends DBFField with DBFText {
  override type T = Int

  def toStructField(): StructField = {
    StructField(name, IntegerType, nullable = true)
  }

  override def readValue(buffer: ByteBuffer): Int = {
    parse(buffer, offset, length, name, 0)(_.toInt)
  }
}

case class FieldLong(name: String, offset: Int, length: Int) extends DBFField with DBFText {
  override type T = Long

  def toStructField(): StructField = {
    StructField(name, LongType, nullable = true)
  }

  override def readValue(buffer: ByteBuffer): Long = {
    parse(buffer, offset, length, name, 0L)(_.toLong)
  }
}

case class FieldFloat(name: String, offset: Int, length: Int) extends DBFField with DBFText {
  override type T = Float

  def toStructField(): StructField = {
    StructField(name, FloatType, nullable = true)
  }

  override def readValue(buffer: ByteBuffer): Float = {
    parse(buffer, offset, length, name, 0.0F)(_.toFloat)
  }
}

case class FieldDouble(name: String, offset: Int, length: Int) extends DBFField with DBFText {
  override type T = Double

  def toStructField(): StructField = {
    StructField(name, DoubleType, nullable = true)
  }

  override def readValue(buffer: ByteBuffer): Double = {
    parse(buffer, offset, length, name, 0.0)(_.toDouble)
  }
}

case class FieldBoolean(name: String, offset: Int, length: Int) extends DBFField with DBFText {
  override type T = Boolean

  def toStructField(): StructField = {
    StructField(name, BooleanType, nullable = true)
  }

  override def readValue(buffer: ByteBuffer): Boolean = {
    // A dbf logical field holds one of T,t,Y,y,F,f,N,n or ? - _not_ "true"/"false".
    val text = readText(buffer, offset, length)
    text.nonEmpty && "TtYy".indexOf(text.charAt(0)) >= 0
  }
}

object DBFField extends Serializable {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  /**
   * Create a DBFField instance.
   *
   * @param stream      the input stream.
   * @param offset      the field offset in the row.
   * @param charsetName the text encoding of the dbf.
   * @return a DBFField instance.
   */
  def apply(stream: FSDataInputStream, offset: Int, charsetName: String): DBFField = {

    val buffer = ByteBuffer.allocate(32).order(ByteOrder.BIG_ENDIAN)
    stream.readFully(buffer.array)

    var nonZeroIndex = 10
    while (nonZeroIndex >= 0 && buffer.get(nonZeroIndex) == 0) {
      nonZeroIndex -= 1
    }
    val fieldName = new String(buffer.array, 0, nonZeroIndex + 1, StandardCharsets.US_ASCII).toLowerCase(Locale.ROOT)
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
      case _ => FieldString(fieldName, offset, fieldLength, charsetName)
    }
  }
}
