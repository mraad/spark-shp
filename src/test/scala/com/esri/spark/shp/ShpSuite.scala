package com.esri.spark.shp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class ShpSuite extends AnyFlatSpec with BeforeAndAfterAll {

  private val folder = "src/test/resources"
  private val path = "src/test/resources/test.shp"
  private val numRec = 3
  private var sparkSession: SparkSession = _

  // Logger.getLogger("com.esri.spark.shp").setLevel(Level.DEBUG)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession
      .builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .master("local")
      .appName("ShpSuite")
      .config("spark.ui.enabled", false)
      .config("spark.sql.warehouse.dir", "/tmp")
      .config("spark.sql.catalogImplementation", "in-memory")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try {
      sparkSession.stop()
    } finally {
      super.afterAll()
    }
  }

  it should "DSL test" in {
    val results = sparkSession
      .sqlContext
      .shp(path)
      .select("*")
      .collect()

    assert(results.size === numRec)
  }

  it should "DDL test" in {
    sparkSession.sql("DROP VIEW IF EXISTS test")
    sparkSession.sql(
      s"""
         |CREATE TEMPORARY VIEW test
         |USING com.esri.spark.shp
         |OPTIONS (path "$path", columns "atext,adate")
        """.stripMargin.replaceAll("\n", " "))

    assert(sparkSession.sql("SELECT atext,adate FROM test").collect().size === numRec)
  }

  it should "DDL test with path as folder" in {
    sparkSession.sql("DROP VIEW IF EXISTS test")
    sparkSession.sql(
      s"""
         |CREATE TEMPORARY VIEW test
         |USING com.esri.spark.shp
         |OPTIONS (path "$folder", columns "adate,along,ashort")
        """.stripMargin.replaceAll("\n", " "))

    assert(sparkSession.sql("SELECT adate,along,ashort FROM test").collect().size === numRec)
  }

  it should "DDL test with path as glob" in {
    sparkSession.sql("DROP VIEW IF EXISTS test")
    sparkSession.sql(
      s"""
         |CREATE TEMPORARY VIEW test
         |USING com.esri.spark.shp
         |OPTIONS (path "$folder/*.shp", columns "adate,along,ashort")
        """.stripMargin.replaceAll("\n", " "))

    assert(sparkSession.sql("SELECT adate,along,ashort FROM test").collect().size === numRec)
  }

}
