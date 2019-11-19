package com.esri.spark.shp

import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.slf4j.LoggerFactory

/**
 * Shapefile Relation
 *
 * @param pathName   path name where shapefiles are located.
 * @param shapeField the name of the shape field.
 * @param columns    Comma separated list of columns to read. "" means all fields.
 */
case class ShpRelation(pathName: String, shapeField: String, columns: String)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  private[shp] def using[A <: {def close() : Unit}, B](r: A)(f: A => B): B = try {
    f(r)
  }
  finally {
    r.close()
  }

  private val arrColumns = columns match {
    case "" => Array.empty[String]
    case _ => columns.split(',')
  }

  override lazy val schema = {
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
            case Some(fileStatus) => {
              logger.debug("Schema is based on {}", fileStatus.getPath.toUri.toString)
              using(DBFFile(fileStatus.getPath, configuration, 0L, arrColumns))(dbfFile => {
                StructType(dbfFile.addFieldTypes(Array(StructField(shapeField, BinaryType))))
              })
            }
            case _ => {
              logger.warn(s"Cannot find a dbf file in $pathName. Creating an empty schema !")
              StructType(Array.empty[StructField])
            }
          }
        }
        else {
          using(DBFFile(pathName.replace(".shp", ""), configuration, 0L, arrColumns))(dbfFile => {
            StructType(dbfFile.addFieldTypes(Array(StructField(shapeField, BinaryType))))
          })
        }
      } else {
        // User provided regexp path, ie. /data/foo*.shp
        fs.globStatus(path)
          .headOption match {
          case Some(fileStatus) => {
            val pathName = fileStatus.getPath.toUri.toString.replace(".shp", "")
            logger.debug("Schema is based on {}", pathName)
            using(DBFFile(pathName, configuration, 0L, arrColumns))(dbfFile => {
              StructType(dbfFile.addFieldTypes(Array(StructField(shapeField, BinaryType))))
            })
          }
          case _ => {
            logger.warn(s"Cannot match file with $pathName. Creating an empty schema !")
            StructType(Array.empty[StructField])
          }
        }
      }
    })
  }

  override def buildScan(): RDD[Row] = {
    ShpRDD(sqlContext.sparkContext, schema, pathName, arrColumns)
  }
}
