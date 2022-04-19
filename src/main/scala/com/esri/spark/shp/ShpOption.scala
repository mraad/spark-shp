package com.esri.spark.shp

/**
 * Options for spark sql read.
 */
object ShpOption extends Serializable {
  val PATH = "path"
  val SHAPE = "shape"

  val COLUMNS = "columns"
  val COLUMNS_ALL = ""

  val FORMAT = "format"
  val FORMAT_SHP = "SHP"
  val FORMAT_WKT = "WKT"
  val FORMAT_WKB = "WKB"
  val FORMAT_GEOJSON = "GEOJSON"

  val REPAIR = "repair"
  val REPAIR_NONE = "none"
  val REPAIR_ESRI = "esri"
  val REPAIR_OGC = "ogc"

  val WKID = "wkid"
  val WKID_NONE = "-1"
}
