package com.esri.spark.shp

import com.esri.core.geometry.Point
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Date

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

  it should "read the attribute values and their types" in {
    val df = sparkSession.read.shp(path)
    val schema = df.schema
    assert(schema.map(_.name) === Seq("shape", "id", "atext", "afloat", "adouble", "ashort", "along", "adate"))
    assert(schema("shape").dataType === BinaryType)
    assert(schema("id").dataType === LongType)
    assert(schema("atext").dataType === StringType)
    assert(schema("afloat").dataType === FloatType)
    assert(schema("adouble").dataType === DoubleType)
    assert(schema("ashort").dataType === ShortType)
    assert(schema("along").dataType === LongType)
    assert(schema("adate").dataType === DateType)

    val rows = df.orderBy("id").collect()
    assert(rows.length === numRec)
    rows.zipWithIndex.foreach { case (row, i) =>
      val n = i + 1
      assert(row.getAs[Long]("id") === i)
      assert(row.getAs[String]("atext") === s"aText$n")
      assert(row.getAs[Float]("afloat") === 10.0F * n)
      assert(row.getAs[Double]("adouble") === 10.0 * n)
      assert(row.getAs[Short]("ashort") === (10 * n).toShort)
      assert(row.getAs[Long]("along") === 10L * n)
      assert(row.getAs[Date]("adate") === Date.valueOf(f"2019-11-${19 + n}%02d"))
      assert(row.getGeometry().isInstanceOf[Point])
    }
  }

  it should "honor the shape format, whatever its case" in {
    // The schema and the row content are derived independently, they have to agree on the format.
    def shapeOf(format: String): (org.apache.spark.sql.types.DataType, Any) = {
      val df = sparkSession.read.shp(path, shapeFormat = format)
      (df.schema("shape").dataType, df.head().get(0))
    }

    val (shpType, shpValue) = shapeOf("SHP")
    assert(shpType === BinaryType)
    assert(shpValue.asInstanceOf[Array[Byte]].length === 20) // 4 byte shape type + 2 doubles.

    val (wkbType, wkbValue) = shapeOf("WKB")
    assert(wkbType === BinaryType)
    assert(wkbValue.isInstanceOf[Array[Byte]])

    val (jsonType, jsonValue) = shapeOf("GEOJSON")
    assert(jsonType === StringType)
    assert(jsonValue.asInstanceOf[String].contains("Point"))

    Seq("WKT", "wkt", "WkT").foreach { format =>
      val (dataType, value) = shapeOf(format)
      assert(dataType === StringType, format)
      assert(value.asInstanceOf[String].startsWith("POINT"), format)
    }
  }

  it should "read every record of every file when given a folder" in {
    val rows = sparkSession.read.shp(folder, shapeFormat = "WKT").collect()
    assert(rows.length === numRec)
    assert(rows.map(_.getString(0)).distinct.length === numRec)
  }

}
