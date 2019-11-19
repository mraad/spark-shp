package com.esri.spark.shp

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

/**
  * Create a Spark SQL Row iterator.
  *
  * @param shpFile shp file reference.
  * @param dbfFile dbf file reference.
  * @param schema  Spark SQL schema.
  */
class ShpIterator(shpFile: ShpFile, dbfFile: DBFFile, schema: StructType) extends Iterator[Row] with Serializable {

  val count = dbfFile.header.numRows
  var index = 0

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
    val shp = shpFile.next
    val dbf = dbfFile.next
    new GenericRowWithSchema(shp +: dbf, schema)
  }

}
