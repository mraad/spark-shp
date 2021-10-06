package com.esri.spark.shp

import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Shapefile Relation
 *
 * @param pathName    The path name where shapefiles are located.
 * @param shapeName   The name of the shape field.
 * @param shapeFormat The shape field output format.
 * @param columns     Comma separated list of columns to read. "" means all fields.
 */
case class ShpRelation(pathName: String,
                       shapeName: String,
                       shapeFormat: String,
                       columns: String
                      )(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {

  private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private[shp] def using[A <: {def close(): Unit}, B](r: A)(f: A => B): B = try {
    f(r)
  }
  finally {
    r.close()
  }

  private val arrColumns = columns match {
    case "" => Array.empty[String]
    case _ => columns.split(',').map(_.toLowerCase)
  }

  override lazy val schema: StructType = {
    val shapeType = shapeFormat match {
      case ShpOption.FORMAT_WKT => StringType
      case ShpOption.FORMAT_GEOJSON => StringType
      case _ => BinaryType
    }
    val configuration = sqlContext.sparkContext.hadoopConfiguration
    val path = new Path(pathName)
    using(path.getFileSystem(configuration))(fs => {
      if (fs.exists(path)) {
        // User provided explict folder or file.
        val status = fs.getFileStatus(path)
        if (status.isDirectory) {
          fs
            .listStatus(path, new PathFilter {
              override def accept(path: Path): Boolean = {
                path.getName.endsWith(".dbf")
              }
            })
            .headOption match {
            case Some(fileStatus) =>
              logger.debug("Schema is based on {}", fileStatus.getPath.toUri.toString)
              using(DBFFile(fileStatus.getPath, configuration, 0L, arrColumns))(dbfFile => {
                StructType(dbfFile.addFieldTypes(Array(StructField(shapeName, shapeType, nullable = true))))
              })
            case _ =>
              logger.warn(s"Cannot find a dbf file in $pathName. Creating an empty schema !")
              StructType(Array.empty[StructField])
          }
        }
        else {
          using(DBFFile(pathName.replace(".shp", ""), configuration, 0L, arrColumns))(dbfFile => {
            StructType(dbfFile.addFieldTypes(Array(StructField(shapeName, shapeType, nullable = true))))
          })
        }
      } else {
        // User provided regexp path, ie. /data/foo*.shp
        fs.globStatus(path)
          .headOption match {
          case Some(fileStatus) =>
            val pathName = fileStatus.getPath.toUri.toString.replace(".shp", "")
            logger.debug("Schema is based on {}", pathName)
            using(DBFFile(pathName, configuration, 0L, arrColumns))(dbfFile => {
              StructType(dbfFile.addFieldTypes(Array(StructField(shapeName, shapeType, nullable = true))))
            })
          case _ =>
            logger.warn(s"Cannot match file with $pathName. Creating an empty schema !")
            StructType(Array.empty[StructField])
        }
      }
    })
  }

  override def buildScan(): RDD[Row] = {
    ShpRDD(sqlContext.sparkContext, schema, pathName, arrColumns, shapeFormat)
  }
}
