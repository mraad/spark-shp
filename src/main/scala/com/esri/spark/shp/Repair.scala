package com.esri.spark.shp

import com.esri.core.geometry.{Geometry, SpatialReference}

/**
 * How to repair a geometry.
 */
trait Repair extends Serializable {
  def repair(geom: Geometry): Geometry
}
