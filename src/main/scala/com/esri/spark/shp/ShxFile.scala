package com.esri.spark.shp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}

/**
  * ShxFile instance.
  *
  * @param shpHeader A ShpHeader instance.
  * @param stream    input stream.
  */
class ShxFile(val shpHeader: ShpHeader,
              stream: FSDataInputStream
             ) extends Serializable with AutoCloseable {

  /**
    * Convert a feature number to a stream seek position.
    *
    * @param featureNum the feature number.
    * @return a stream seek position.
    */
  def rowNumToSeekPos(featureNum: Long): Long = {
    stream.seek(100L + featureNum * 8L)
    stream.readInt() * 2L
  }

  /**
    * Close the input stream.
    */
  override def close(): Unit = {
    stream.close()
  }

}

/**
  * Supporting class object.
  */
object ShxFile extends Serializable {

  /**
    * Get the number of features given a shapefile.
    *
    * @param pathName the path to the shapefile.
    * @param conf     Hadoop configuration reference.
    * @return the number of features in the shapefile.
    */
  def getNumRows(pathName: String, conf: Configuration): Long = {
    // TODO, handle case when user passes .shp ext.
    val path = new Path(pathName + ".shx")
    val status = path.getFileSystem(conf).getFileStatus(path)
    (status.getLen - 100L) / 8L
  }

  /**
    * Create a ShxFile instance.
    *
    * @param pathName the shapefile path without .shp ext.
    * @param conf     Hadoop configuration reference.
    * @return ShxFile instance.
    */
  def apply(pathName: String, conf: Configuration): ShxFile = {
    val path = new Path(pathName + ".shx")
    val stream = path.getFileSystem(conf).open(path)
    val shpHeader = ShpHeader(stream)
    new ShxFile(shpHeader, stream)
  }

}
