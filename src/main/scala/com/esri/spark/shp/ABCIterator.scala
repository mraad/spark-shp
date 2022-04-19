package com.esri.spark.shp

import com.esri.core.geometry._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

import java.nio.{ByteBuffer, ByteOrder}

/**
 * Create an abstract Spark SQL Row iterator.
 *
 * @param shpFile shp file reference.
 * @param dbfFile dbf file reference.
 * @param schema  Spark SQL schema.
 */
abstract class ABCIterator[T](shpFile: ShpFile, dbfFile: DBFFile, schema: StructType)
  extends Iterator[Row] with Serializable {

  val count: Int = dbfFile.header.numRows
  var index = 0

  /**
   * Map bytes to explicit geometry type.
   *
   * @param bytes array of bytes.
   * @return A T instance.
   */
  def map(bytes: Array[Byte]): T

  /**
   * @return true if iterator has more rows, false otherwise.
   */
  override def hasNext: Boolean = {
    index < count
  }

  /**
   * @return a Spark SQL Row instance.
   */
  override def next(): Row = {
    index += 1
    val shp = map(shpFile.next())
    val dbf = dbfFile.next()
    new GenericRowWithSchema(shp +: dbf, schema)
  }

}

/**
 * Iterator to return the original array of bytes.
 */
class ShpIterator(shpFile: ShpFile, dbfFile: DBFFile, schema: StructType)
  extends ABCIterator[Array[Byte]](shpFile, dbfFile, schema) {

  override def map(bytes: Array[Byte]): Array[Byte] = bytes
}

/**
 * Iterator to return the geometry in WKB format.
 */
class WKBIterator(shpFile: ShpFile,
                  dbfFile: DBFFile,
                  schema: StructType,
                  repair: Repair)
  extends ABCIterator[Array[Byte]](shpFile, dbfFile, schema) {

  private val opShp = OperatorImportFromESRIShape.local
  private val opExp = OperatorExportToWkb.local

  override def map(bytes: Array[Byte]): Array[Byte] = {
    val geometry = opShp.execute(ShapeImportFlags.ShapeImportNonTrusted,
      Geometry.Type.Unknown,
      ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN))
    opExp.execute(ShapeExportFlags.ShapeExportDefaults,
      repair.repair(geometry),
      null).array()
  }
}

/**
 * Iterator to return the geometry in WKT format.
 */
class WKTIterator(shpFile: ShpFile,
                  dbfFile: DBFFile,
                  schema: StructType,
                  repair: Repair)
  extends ABCIterator[String](shpFile, dbfFile, schema) {

  private val opShp = OperatorImportFromESRIShape.local
  private val opExp = OperatorExportToWkt.local

  override def map(bytes: Array[Byte]): String = {
    val geometry = opShp.execute(ShapeImportFlags.ShapeImportNonTrusted,
      Geometry.Type.Unknown,
      ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN))
    opExp.execute(ShapeExportFlags.ShapeExportDefaults,
      repair.repair(geometry),
      null)
  }
}

/**
 * Iterator to return the geometry in GeoJSON format.
 */
class GeoJSONIterator(shpFile: ShpFile,
                      dbfFile: DBFFile,
                      schema: StructType,
                      repair: Repair)
  extends ABCIterator[String](shpFile, dbfFile, schema) {

  private val opShp = OperatorImportFromESRIShape.local
  private val opExp = OperatorExportToGeoJson.local

  override def map(bytes: Array[Byte]): String = {
    val geometry = opShp.execute(ShapeImportFlags.ShapeImportNonTrusted,
      Geometry.Type.Unknown,
      ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN))
    opExp.execute(repair.repair(geometry))
  }
}
