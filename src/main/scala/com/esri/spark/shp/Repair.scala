package com.esri.spark.shp

import com.esri.core.geometry.{Geometry, OperatorSimplify, OperatorSimplifyOGC, SpatialReference}

/**
 * How to repair a geometry.
 */
trait Repair extends Serializable {
  def repair(geom: Geometry): Geometry
}

/**
 * Supporting class object.
 */
object Repair extends Serializable {

  private val wkidRegex = """(\d+)""".r

  /**
   * Create a Repair instance.
   *
   * @param mode one of ShpOption.REPAIR_NONE, ShpOption.REPAIR_ESRI or ShpOption.REPAIR_OGC.
   * @param wkid a numerical spatial reference identifier, a well known text, or ShpOption.WKID_NONE.
   * @return a Repair instance.
   */
  def apply(mode: String, wkid: String): Repair = {
    val sr = wkid match {
      case ShpOption.WKID_NONE => null
      case wkidRegex(value) => SpatialReference.create(value.toInt)
      case text => SpatialReference.create(text)
    }
    mode match {
      case ShpOption.REPAIR_ESRI =>
        val operator = OperatorSimplify.local()
        geom => operator.execute(geom, sr, true, null)
      case ShpOption.REPAIR_OGC =>
        val operator = OperatorSimplifyOGC.local()
        geom => operator.execute(geom, sr, true, null)
      case _ =>
        geom => geom
    }
  }
}
