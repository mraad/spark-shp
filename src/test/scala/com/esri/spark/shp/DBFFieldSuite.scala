package com.esri.spark.shp

import org.scalatest.flatspec.AnyFlatSpec

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.Date

/**
 * DBFField tests that need no Spark session, covering the values the test.dbf fixture does not hold.
 */
class DBFFieldSuite extends AnyFlatSpec {

  private def buffer(text: String, charset: String = "US-ASCII"): ByteBuffer =
    ByteBuffer.wrap(text.getBytes(charset))

  it should "read a dbf logical field" in {
    // A dbf logical is T/t/Y/y/F/f/N/n/?, _not_ "true"/"false".
    val field = FieldBoolean("alogical", 0, 1)
    assert(field.readValue(buffer("T")) === true)
    assert(field.readValue(buffer("t")) === true)
    assert(field.readValue(buffer("Y")) === true)
    assert(field.readValue(buffer("F")) === false)
    assert(field.readValue(buffer("N")) === false)
    assert(field.readValue(buffer("?")) === false)
    assert(field.readValue(buffer(" ")) === false)
  }

  it should "default a blank value without throwing" in {
    assert(FieldShort("f", 0, 5).readValue(buffer("     ")) === 0)
    assert(FieldInt("f", 0, 5).readValue(buffer("     ")) === 0)
    assert(FieldLong("f", 0, 5).readValue(buffer("     ")) === 0L)
    assert(FieldFloat("f", 0, 5).readValue(buffer("     ")) === 0.0F)
    assert(FieldDouble("f", 0, 5).readValue(buffer("     ")) === 0.0)
    assert(FieldDate("f", 0, 8).readValue(buffer("        ")) === null)
  }

  it should "default a malformed value" in {
    assert(FieldInt("f", 0, 5).readValue(buffer("bogus")) === 0)
    assert(FieldDate("f", 0, 8).readValue(buffer("notadate")) === null)
  }

  it should "trim blanks and nulls" in {
    assert(FieldString("f", 0, 8, "US-ASCII").readValue(buffer("  text  ")) === "text")
    assert(FieldString("f", 0, 8, "US-ASCII").readValue(ByteBuffer.wrap("text".getBytes ++ Array[Byte](0, 0, 0, 0))) === "text")
  }

  it should "honor the field charset" in {
    val text = "Ålesund"
    val utf8 = text.getBytes(StandardCharsets.UTF_8)
    assert(FieldString("f", 0, utf8.length, "UTF-8").readValue(ByteBuffer.wrap(utf8)) === text)

    val latin1 = text.getBytes(StandardCharsets.ISO_8859_1)
    assert(FieldString("f", 0, latin1.length, "ISO-8859-1").readValue(ByteBuffer.wrap(latin1)) === text)
    // The same bytes read as the wrong charset do _not_ round trip - hence reading the .cpg side file.
    assert(FieldString("f", 0, latin1.length, "UTF-8").readValue(ByteBuffer.wrap(latin1)) !== text)
  }

  it should "read a field at an offset" in {
    val buf = buffer("xx20191120yy")
    assert(FieldDate("adate", 2, 8).readValue(buf) === Date.valueOf("2019-11-20"))
  }
}
