package com.esri.spark.shp

import org.apache.hadoop.conf.Configuration
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
 * A partition covers one shapefile.
 *
 * @param index    the partition index.
 * @param pathName the shapefile path _without_ the .shp extension.
 */
case class ShpPartition(index: Int, pathName: String) extends Partition

/**
 * @param shapeFormat the shape output format, uppercased.
 * @param repairMode  the geometry repair mode, lowercased.
 */
case class ShpRDD(@transient sc: SparkContext,
                  hadoopConfSer: SerializableConfiguration,
                  schema: StructType,
                  pathName: String,
                  columns: Array[String],
                  shapeFormat: String,
                  repairMode: String,
                  wkid: String
                 ) extends RDD[Row](sc, Nil) {

  @DeveloperApi
  override def compute(partition: Partition,
                       context: TaskContext
                      ): Iterator[Row] = {
    partition match {
      case part: ShpPartition =>
        if (log.isDebugEnabled) {
          schema.printTreeString()
        }
        log.debug("compute::Reading {}", part.pathName)
        val conf = hadoopConfSer.value
        val shpFile = ShpFile(part.pathName, conf)
        val dbfFile = DBFFile(part.pathName, conf, columns)
        context.addTaskCompletionListener[Unit](_ => {
          shpFile.close()
          dbfFile.close()
        })
        lazy val repair = Repair(repairMode, wkid)
        shapeFormat match {
          case ShpOption.FORMAT_WKT => new WKTIterator(shpFile, dbfFile, schema, repair)
          case ShpOption.FORMAT_WKB => new WKBIterator(shpFile, dbfFile, schema, repair)
          case ShpOption.FORMAT_GEOJSON => new GeoJSONIterator(shpFile, dbfFile, schema, repair)
          case _ => new ShpIterator(shpFile, dbfFile, schema)
        }
      case _ => Iterator.empty
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val conf = if (sc == null) new Configuration() else sc.hadoopConfiguration
    resolveShpPaths(pathName, conf)
      .zipWithIndex
      .map { case (basePath, index) => ShpPartition(index, basePath): Partition }
  }
}
