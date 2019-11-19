package com.esri.spark.shp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Partition, SparkContext, TaskContext}

case class ShpPartition(index: Int, pathName: String) extends Partition

case class ShpRDD(@transient sc: SparkContext,
                  schema: StructType,
                  pathName: String
                 ) extends RDD[Row](sc, Nil) {

  @DeveloperApi
  override def compute(partition: Partition, context: TaskContext): Iterator[Row] = {
    partition match {
      case part: ShpPartition => {
        val hadoopConf = if (sc == null) new Configuration() else sc.hadoopConfiguration
        log.info("Reading {}", part.pathName)
        val shpFile = ShpFile(part.pathName, hadoopConf, 0L)
        val dbfFile = DBFFile(part.pathName, hadoopConf, 0L)
        context.addTaskCompletionListener(_ => {
          shpFile.close()
          dbfFile.close()
        })
        new ShpIterator(shpFile, dbfFile, schema)
      }
      case _ => Iterator.empty
    }
  }

  private[shp] def using[A <: {def close() : Unit}, B](r: A)(f: A => B): B = try {
    f(r)
  }
  finally {
    r.close()
  }

  override protected def getPartitions: Array[Partition] = {
    val conf = if (sc == null) new Configuration() else sc.hadoopConfiguration
    val path = new Path(pathName)
    using(path.getFileSystem(conf))(fs => {
      if (fs.exists(path)) {
        // User passed a directory or a file.
        if (fs.getFileStatus(path).isDirectory) {
          // Get all the shp files in the directory.
          fs
            .listStatus(path, new PathFilter {
              override def accept(path: Path): Boolean = {
                path.getName.endsWith(".shp")
              }
            })
            .zipWithIndex
            .map {
              case (fileStatus, index) => {
                ShpPartition(index, fileStatus.getPath.toUri.toURL.toString.replace(".shp", ""))
              }
            }
        }
        else {
          // User passed a file.
          Array(ShpPartition(0, pathName.replace(".shp", "")))
        }
      } else {
        // User passed a regexp, something like /data/foo*.shp
        fs.globStatus(path)
          .zipWithIndex
          .map {
            case (fileStatus, index) => {
              ShpPartition(index, fileStatus.getPath.toUri.toURL.toString.replace(".shp", ""))
            }
          }
      }
    })
  }
}
