package com.esri.spark

import java.nio.{ByteBuffer, ByteOrder}

import com.esri.core.geometry._
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SQLContext}

package object shp {

  implicit class RowImplicits(row: Row) extends Serializable {
    private val op = OperatorFactoryLocal.getInstance.getOperator(Operator.Type.ImportFromESRIShape).asInstanceOf[OperatorImportFromESRIShape]

    /**
     * Get Geometry instance from SQL Row.
     * It is assumed that the first field contains the geometry as an array of bytes in ESRI binary format.
     *
     * @param index the field index. Default = 0.
     * @return Geometry instance.
     */
    def getGeometry(index: Int = 0): Geometry = {
      // GeometryEngine.geometryFromEsriShape(row.getAs[Array[Byte]](0), Geometry.Type.Unknown)
      val esriShapeBuffer = row.getAs[Array[Byte]](index)
      op.execute(ShapeImportFlags.ShapeImportNonTrusted, Geometry.Type.Unknown, ByteBuffer.wrap(esriShapeBuffer).order(ByteOrder.LITTLE_ENDIAN))
    }
  }

  implicit class SQLContextImplicits(sqlContext: SQLContext) extends Serializable {
    def shp(pathName: String, shapeField: String = ShpOption.SHAPE): DataFrame = {
      sqlContext.baseRelationToDataFrame(ShpRelation(pathName, shapeField)(sqlContext))
    }
  }

  implicit class DataFrameReaderImplicits(dataFrameReader: DataFrameReader) extends Serializable {
    def shp(pathName: String, shapeField: String = ShpOption.SHAPE): DataFrame = {
      dataFrameReader
        .format("com.esri.shp")
        .option(ShpOption.PATH, pathName)
        .option(ShpOption.SHAPE, shapeField)
        .load()
    }
  }

}
