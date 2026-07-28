package com.esri.spark

import com.esri.core.geometry._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path, PathFilter}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SQLContext}

import java.nio.{ByteBuffer, ByteOrder}

package object shp {

  private val shpFilter = new PathFilter {
    override def accept(path: Path): Boolean = path.getName.endsWith(".shp")
  }

  /**
   * Apply a function to a closeable resource and close it afterwards.
   */
  private[shp] def using[A <: AutoCloseable, B](r: A)(f: A => B): B = try {
    f(r)
  } finally {
    r.close()
  }

  /**
   * Resolve a user supplied path to the base path names of the shapefiles it refers to,
   * where a base path name is the path _without_ the .shp extension.
   *
   * The path can be a folder, an explicit .shp file, or a globbing expression like /data/foo*.shp.
   *
   * @param pathName the user supplied path.
   * @param conf     Hadoop configuration reference.
   * @return the base path name of each shapefile, in a stable order.
   */
  private[shp] def resolveShpPaths(pathName: String, conf: Configuration): Array[String] = {
    val path = new Path(pathName)
    // Note, do _not_ close this FileSystem, it is a JVM wide cached instance shared with everybody else.
    val fs = path.getFileSystem(conf)
    val statuses: Array[FileStatus] =
      if (!fs.exists(path)) {
        // User passed a globbing expression, ie. /data/foo*.shp
        Option(fs.globStatus(path, shpFilter)).getOrElse(Array.empty)
      } else if (fs.getFileStatus(path).isDirectory) {
        fs.listStatus(path, shpFilter)
      } else {
        Array(fs.getFileStatus(path))
      }
    statuses.map(_.getPath.toUri.toString.stripSuffix(".shp")).sorted
  }

  implicit class RowImplicits(val row: Row) extends AnyVal {

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
