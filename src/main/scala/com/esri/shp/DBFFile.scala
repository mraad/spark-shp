package com.esri.shp

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.spark.sql.types.StructField

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
             ) extends Serializable with AutoCloseable {

  private val buffer = ByteBuffer.allocate(header.rowLength).order(ByteOrder.BIG_ENDIAN)

  def addFieldTypes(arr: Array[StructField]): Array[StructField] = {
    fields.foldLeft(arr)((arr, field) => {
      arr :+ field.toStructField()
    })
  }

  def next(): Array[Any] = {
    stream.readFully(buffer.array)
    fields.map(_.readValue(buffer))
  }

  /**
    * Close the input stream.
    */
  override def close() = {
    stream.close()
  }
}

/**
  * Support class object.
  */
object DBFFile extends Serializable {

  /**
    * Create DBFFile instance.
    *
    * @param pathName the base path to dbf file _without_ the .dbf extension.
    * @param conf     Hadoop configuration reference.
    * @param startRow The starting row.
    * @return DBFFile instance.
    */
  def apply(pathName: String, conf: Configuration, startRow: Long): DBFFile = {
    // TODO Handle case where user passes the full path + ext.
    apply(new Path(pathName + ".dbf"), conf, startRow)
  }

  /**
    * Create DBFFile instance.
    *
    * @param path     Path instance to the dbf file.
    * @param conf     Hadoop configuration reference.
    * @param startRow the starting row.
    * @return DBFFile instance.
    */
  def apply(path: Path, conf: Configuration, startRow: Long): DBFFile = {
    val stream = path.getFileSystem(conf).open(path)
    val header = DBFHeader(stream)
    val (_, fields) = (1 to header.numFields).foldLeft(
      (1, Array.empty[DBFField])) {
      case ((offset, fields), _) => {
        val field = DBFField(stream, offset)
        (offset + field.length, fields :+ field)
      }
    }
    stream.seek(header.rowNumToSeekPos(startRow))
    new DBFFile(header, fields, stream)
  }
}