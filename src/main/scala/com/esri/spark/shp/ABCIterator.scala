package com.esri.spark.shp

import com.esri.core.geometry._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

import java.nio.ByteBuffer
import java.util.Arrays

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

  private val numCols = dbfFile.fields.length + 1

  /**
   * Map the Esri shape bytes to an explicit geometry type.
   *
   * Note, the buffer is reused between calls, do not retain a reference to it.
   *
   * @param buffer the Esri shape bytes.
   * @return A T instance.
   */
  def map(buffer: ByteBuffer): T

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
    val values = new Array[Any](numCols)
    values(0) = map(shpFile.next())
    dbfFile.next(values, 1)
    new GenericRowWithSchema(values, schema)
  }

}

/**
 * Iterator to return the original array of bytes.
 */
class ShpIterator(shpFile: ShpFile, dbfFile: DBFFile, schema: StructType)
  extends ABCIterator[Array[Byte]](shpFile, dbfFile, schema) {

  // The bytes outlive the call as they end up in the Row, so they have to be copied out of the shared buffer.
  override def map(buffer: ByteBuffer): Array[Byte] = Arrays.copyOf(buffer.array, buffer.limit)
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

  override def map(buffer: ByteBuffer): Array[Byte] = {
    val geometry = opShp.execute(ShapeImportFlags.ShapeImportNonTrusted, Geometry.Type.Unknown, buffer)
    opExp.execute(ShapeExportFlags.ShapeExportDefaults, repair.repair(geometry), null).array()
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

  override def map(buffer: ByteBuffer): String = {
    val geometry = opShp.execute(ShapeImportFlags.ShapeImportNonTrusted, Geometry.Type.Unknown, buffer)
    opExp.execute(ShapeExportFlags.ShapeExportDefaults, repair.repair(geometry), null)
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

  override def map(buffer: ByteBuffer): String = {
    val geometry = opShp.execute(ShapeImportFlags.ShapeImportNonTrusted, Geometry.Type.Unknown, buffer)
    opExp.execute(repair.repair(geometry))
  }
}
