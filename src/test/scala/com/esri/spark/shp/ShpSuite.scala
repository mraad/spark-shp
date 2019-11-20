package com.esri.spark.shp

import java.io.File
import java.nio.charset.UnsupportedCharsetException
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import scala.io.Source
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.Matchers._

class ShpSuite extends FunSuite with BeforeAndAfterAll {

  private val path = "src/test/resources/test.shp"
  private val numRec = 3
  private var sparkSession: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession
      .builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .master("local[*]")
      .appName("ShpSuite")
      .config("spark.ui.enabled", false)
      .config("spark.sql.warehouse.dir", "/tmp")
      // .enableHiveSupport()
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try {
      sparkSession.stop()
    } finally {
      super.afterAll()
    }
  }

  test("DSL test") {
    val results = sparkSession
      .sqlContext
      .shp(path)
      .select("aText")
      .collect()

    assert(results.size === numRec)
  }

}
