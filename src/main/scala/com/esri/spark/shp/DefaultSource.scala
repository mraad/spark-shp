package com.esri.spark.shp

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
 * Provides access to Shapefile data from pure SQL statements.
 */
class DefaultSource extends RelationProvider with SchemaRelationProvider with DataSourceRegister {

  /**
   * @return the short name of the datasource.
   */
  override def shortName(): String = "shp"

  /**
   * Creates a new relation for data store in Shapefile given parameters.
   * Parameters must include 'path' and 'name'.
   */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]
                             ): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   * Creates a new relation for data store in Shapefile given parameters and user supported schema.
   * Parameters must include 'path'.
   */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType
                             ): BaseRelation = {
    val logger = LoggerFactory.getLogger(getClass)
    val path = parameters.getOrElse(ShpOption.PATH, sys.error(f"Parameter '${ShpOption.PATH}' must be defined."))
    val shape = parameters.getOrElse(ShpOption.SHAPE, ShpOption.SHAPE)
    val format = parameters.getOrElse(ShpOption.FORMAT, ShpOption.FORMAT_WKB)
    val columns = parameters.getOrElse(ShpOption.COLUMNS, "")
    val repair = parameters.getOrElse(ShpOption.REPAIR, ShpOption.REPAIR_NONE)
    val wkid = parameters.getOrElse(ShpOption.WKID, ShpOption.WKID_NONE)
    if (logger.isDebugEnabled) {
      logger.debug(s"${ShpOption.PATH} = $path")
      logger.debug(s"${ShpOption.SHAPE} = $shape")
      logger.debug(s"${ShpOption.COLUMNS} = $columns")
      logger.debug(s"${ShpOption.REPAIR} = $repair")
      logger.debug(s"${ShpOption.WKID} = $wkid")
    }
    ShpRelation(path, shape, format, columns, repair, wkid)(sqlContext)
  }
}
