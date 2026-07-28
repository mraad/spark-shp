package com.esri.spark.shp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.{Logger, LoggerFactory}

import java.util.Locale

/**
 * Shapefile Relation.
 *
 * @param pathName    The path name where shapefiles are located.
 * @param shapeName   The name of the shape field.
 * @param shapeFormat The shape field output format.
 * @param columns     Comma separated list of columns to read. "" means all fields.
 * @param repair      Repair mode, none, esri, ogc.
 * @param wkid        The spatial reference identifier.
 */
case class ShpRelation(pathName: String,
                       shapeName: String,
                       shapeFormat: String,
                       columns: String,
                       repair: String,
                       wkid: String
                      )
                      (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {

  private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // Normalize here rather than at the entry points, as a relation can be built without going through DefaultSource.
  private val format = shapeFormat.toUpperCase(Locale.ROOT)
  private val repairMode = repair.toLowerCase(Locale.ROOT)

  private val arrColumns = columns match {
    case "" => Array.empty[String]
    case _ => columns.split(',').map(_.trim.toLowerCase(Locale.ROOT))
  }

  override lazy val schema: StructType = {
    val shapeType = format match {
      case ShpOption.FORMAT_WKT | ShpOption.FORMAT_GEOJSON => StringType
      case _ => BinaryType
    }
    val configuration = sqlContext.sparkContext.hadoopConfiguration
    resolveShpPaths(pathName, configuration).headOption match {
      case Some(basePath) =>
        logger.debug("Schema is based on {}", basePath)
        using(DBFFile(basePath, configuration, arrColumns))(dbfFile => {
          StructType(StructField(shapeName, shapeType, nullable = true) +: dbfFile.fields.map(_.toStructField()))
        })
      case None =>
        logger.warn(s"Cannot find a shapefile matching $pathName. Creating an empty schema !")
        StructType(Array.empty[StructField])
    }
  }

  override def buildScan(): RDD[Row] = {
    ShpRDD(
      sqlContext.sparkContext,
      new SerializableConfiguration(sqlContext.sparkContext.hadoopConfiguration),
      schema, pathName, arrColumns, format, repairMode, wkid)
  }
}
