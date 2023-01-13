package com.esri.spark

import java.nio.{ByteBuffer, ByteOrder}

import com.esri.core.geometry._
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SQLContext}

package object shp {

  implicit class RowImplicits(val row: Row) extends AnyVal {
    // private val op = OperatorFactoryLocal.getInstance.getOperator(Operator.Type.ImportFromESRIShape).asInstanceOf[OperatorImportFromESRIShape]

    /**
     * Get Geometry instance from SQL Row.
     * It is assumed that the first field contains the geometry as an array of bytes in ESRI binary format.
     *
     * @param index the field index. Default = 0.
     * @return Geometry instance.
     */
    def getGeometry(index: Int = 0): Geometry = {
      val esriShapeBuffer = row.getAs[Array[Byte]](index)
      OperatorImportFromESRIShape
        .local()
        .execute(ShapeImportFlags.ShapeImportNonTrusted,
          Geometry.Type.Unknown,
          ByteBuffer.wrap(esriShapeBuffer).order(ByteOrder.LITTLE_ENDIAN))
    }
  }

  implicit class SQLContextImplicits(val sqlContext: SQLContext) extends AnyVal {
    def shp(pathName: String,
            shapeName: String = ShpOption.SHAPE,
            shapeFormat: String = ShpOption.FORMAT_WKB,
            columns: String = ShpOption.COLUMNS_ALL,
            repair: String = ShpOption.REPAIR_NONE,
            wkid: String = ShpOption.WKID_NONE
           ): DataFrame = {
      sqlContext.baseRelationToDataFrame(ShpRelation(pathName,
        shapeName,
        shapeFormat,
        columns,
        repair,
        wkid)(sqlContext))
    }
  }

  implicit class DataFrameReaderImplicits(val dataFrameReader: DataFrameReader) extends AnyVal {
    def shp(pathName: String,
            shapeName: String = ShpOption.SHAPE,
            shapeFormat: String = ShpOption.FORMAT_WKB,
            columns: String = ShpOption.COLUMNS_ALL,
            repair: String = ShpOption.REPAIR_NONE,
            wkid: String = ShpOption.WKID_NONE
           ): DataFrame = {
      dataFrameReader
        .format("shp")
        .option(ShpOption.PATH, pathName)
        .option(ShpOption.SHAPE, shapeName)
        .option(ShpOption.FORMAT, shapeFormat)
        .option(ShpOption.COLUMNS, columns)
        .option(ShpOption.REPAIR, repair)
        .option(ShpOption.WKID, wkid)
        .load()
    }
  }

}
