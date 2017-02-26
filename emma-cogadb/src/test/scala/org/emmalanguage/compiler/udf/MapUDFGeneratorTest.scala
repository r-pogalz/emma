package org.emmalanguage
package compiler.udf

import org.emmalanguage.compiler.lang.cogadb.ast.{MapUdfOutAttr, _}
import org.emmalanguage.compiler.udf.MapUDFGeneratorTest._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.language.implicitConversions
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

@RunWith(classOf[JUnitRunner])
class MapUDFGeneratorTest extends FlatSpec with Matchers with BeforeAndAfter {

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  val symbolTable = Map[String, String](
    "p" -> "PART"
  )

  implicit def MapUdfWrapper(t: MapUdf) = new MapUdfHelperClass(t)

  final class MapUdfHelperClass(udf: MapUdf) {
    implicit def concatenated: String = udf.mapUdfCode.map(_.code).mkString
  }
  

  "MapUDFGenerator" should
    "compile final if-then-else with single then and else statement and consider double cast" in {
    val ast = typecheck(reify {
      () => (p: Part) => if (p.p_partkey > 5) p.p_size else p.p_retailprice
    }.tree)

    val actual = new MapUDFGenerator(ast, symbolTable).generate

    val expectedOutputIde = nextExpectedOutputIdentifier
    val expectedLocalVar = nextExpectedLocalVarIdentifier
    val expectedUDF = s"double $expectedLocalVar;" +
      s"if((#PART.P_PARTKEY#>5)){" +
      s"$expectedLocalVar=(double)(#PART.P_SIZE#);" +
      s"}else{" +
      s"$expectedLocalVar=#PART.P_RETAILPRICE#;" +
      s"}" +
      s"#<OUT>.$expectedOutputIde#=$expectedLocalVar;"

    actual.concatenated should be(expectedUDF)
  }

  "MapUDFGenerator" should "compile a UDF with a single basic type as input" in {

    val astWithShort = typecheck(reify {
      () => (s: Short) => s
    }.tree)
    val actualShortUDF = new MapUDFGenerator(astWithShort, Map[String, String]("s" -> "SHORT")).
                         generate
    val expectedShortUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#SHORT.VALUE#;"
    actualShortUDF.concatenated should be(expectedShortUDF)
    actualShortUDF.mapUdfOutAttr.size should be(1)

    val astWithInt = typecheck(reify {
      () => (i: Int) => i
    }.tree)
    val actualIntUDF = new MapUDFGenerator(astWithInt, Map[String, String]("i" -> "INT")).generate
    val expectedIntUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#INT.VALUE#;"
    actualIntUDF.concatenated should be(expectedIntUDF)
    actualIntUDF.mapUdfOutAttr.size should be(1)

    val astWithLong = typecheck(reify {
      () => (l: Long) => l
    }.tree)
    val actualLongUDF = new MapUDFGenerator(astWithLong, Map[String, String]("l" -> "LONG")).generate
    val expectedLongUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#LONG.VALUE#;"
    actualLongUDF.concatenated should be(expectedLongUDF)
    actualLongUDF.mapUdfOutAttr.size should be(1)

    val astWithFloat = typecheck(reify {
      () => (f: Float) => f
    }.tree)
    val actualFloatUDF = new MapUDFGenerator(astWithFloat, Map[String, String]("f" -> "FLOAT")).generate
    val expectedFloatUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#FLOAT.VALUE#;"
    actualFloatUDF.concatenated should be(expectedFloatUDF)
    actualFloatUDF.mapUdfOutAttr.size should be(1)

    val astWithDouble = typecheck(reify {
      () => (d: Double) => d
    }.tree)
    val actualDoubleUDF = new MapUDFGenerator(astWithDouble, Map[String, String]("d" -> "DOUBLE")).generate
    val expectedDoubleUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#DOUBLE.VALUE#;"
    actualDoubleUDF.concatenated should be(expectedDoubleUDF)
    actualDoubleUDF.mapUdfOutAttr.size should be(1)

    val astWithBoolean = typecheck(reify {
      () => (b: Boolean) => b
    }.tree)
    val actualBoolUDF = new MapUDFGenerator(astWithBoolean, Map[String, String]("b" -> "BOOL")).generate
    val expectedBoolUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#BOOL.VALUE#;"
    actualBoolUDF.concatenated should be(expectedBoolUDF)
    actualBoolUDF.mapUdfOutAttr.size should be(1)

    val astWithChar = typecheck(reify {
      () => (c: Char) => c
    }.tree)
    val actualCharUDF = new MapUDFGenerator(astWithChar, Map[String, String]("c" -> "CHAR")).generate
    val expectedCharUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#CHAR.VALUE#;"
    actualCharUDF.concatenated should be(expectedCharUDF)
    actualCharUDF.mapUdfOutAttr.size should be(1)

    val astWithString = typecheck(reify {
      () => (str: String) => str
    }.tree)
    val actualStringUDF = new MapUDFGenerator(astWithString, Map[String, String]("str" -> "VARCHAR")).generate
    val expectedStringUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#VARCHAR.VALUE#;"
    actualStringUDF.concatenated should be(expectedStringUDF)
    actualStringUDF.mapUdfOutAttr.size should be(1)

  }

  "MapUDFGenerator" should "compile a UDF with a Tuple input containing basic types" in {

    val ast = typecheck(reify {
      () => (input: (Int, Double, String)) => input._1 + input._2
    }.tree)

    val symTbl = Map[String, String]("input" -> "TRIPLE")
    val actual = new MapUDFGenerator(ast, symTbl).generate

    val expectedUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=(#TRIPLE._1#+#TRIPLE._2#);"

    actual.concatenated should be(expectedUDF)
    actual.mapUdfOutAttr.size should be(1)
  }

  "MapUDFGenerator" should "compile a UDF with a Tuple input containing case classes and basic types" in {

    val ast = typecheck(reify {
      () => (input: (Part, Double, String)) => input._1.p_retailprice + input._2
    }.tree)

    val symTbl = Map[String, String]("input" -> "TRIPLE")
    val actual = new MapUDFGenerator(ast, symTbl).generate

    val expectedUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=(#TRIPLE._1_P_RETAILPRICE#+#TRIPLE._2#);"

    actual.concatenated should be(expectedUDF)
    actual.mapUdfOutAttr.size should be(1)
  }

  "MapUDFGenerator" should "return correct result type" in {

    val ast = typecheck(reify {
      () => (p: Part) => p.p_size * p.p_retailprice
    }.tree)

    val actual = new MapUDFGenerator(ast, symbolTable).generate

    val expResIdentifier = nextExpectedOutputIdentifier
    val expectedUDF = s"#<OUT>.$expResIdentifier#=(#PART.P_SIZE#*#PART.P_RETAILPRICE#);"

    actual.concatenated should be(expectedUDF)
    actual.mapUdfOutAttr.size should be(1)
    actual.mapUdfOutAttr.head should be(MapUdfOutAttr("DOUBLE", expResIdentifier, expResIdentifier))
  }

  "MapUDFGenerator" should "consider projection of case class access to a basic type" in {

    val ast = typecheck(reify {
      () => (p: Part) => p.p_size
    }.tree)

    val actual = new MapUDFGenerator(ast, symbolTable).generate

    val expResIdentifier = nextExpectedOutputIdentifier
    val expectedUDF = s"#<OUT>.$expResIdentifier#=#PART.P_SIZE#;"

    actual.concatenated should be(expectedUDF)
    actual.mapUdfOutAttr.size should be(1)
    actual.mapUdfOutAttr.head should be(MapUdfOutAttr("INT", expResIdentifier, expResIdentifier))
  }

  "MapUDFGenerator" should "return multiple results for a Complex output type" in {

    val ast = typecheck(reify {
      () => (p: Part) => PartPrice(1, p.p_retailprice * p.p_size)
    }.tree)

    val actual = new MapUDFGenerator(ast, symbolTable).generate

    val expResIdentifier1 = "MAP_UDF_RES_PART_PRICE_KEY_1"
    val expResIdentifier2 = "MAP_UDF_RES_PART_PRICE_AMOUNT_1"
    val expectedUDF = s"#<OUT>.$expResIdentifier1#=1;" +
      s"#<OUT>.$expResIdentifier2#=(#PART.P_RETAILPRICE#*#PART.P_SIZE#);"
    val expectedOutputs = Seq(
      MapUdfOutAttr("INT", expResIdentifier1, expResIdentifier1),
      MapUdfOutAttr("DOUBLE", expResIdentifier2, expResIdentifier2))

    actual.concatenated should be(expectedUDF)
    actual.mapUdfOutAttr.size should be(2)
    for (expected <- expectedOutputs) actual.mapUdfOutAttr should contain(expected)
  }

  "MapUDFGenerator" should "flatten a nested Complex output" in {

    val ast = typecheck(reify {
      () => (p: Part) => Nested(Part(0, "o", 2, 0.5, "comment"))
    }.tree)

    val actual = new MapUDFGenerator(ast, symbolTable).generate

    val expectedUDF = "#<OUT>.MAP_UDF_RES_P_P_PARTKEY_1#=0;" +
      "#<OUT>.MAP_UDF_RES_P_P_NAME_1#=\"o\";" +
      "#<OUT>.MAP_UDF_RES_P_P_SIZE_1#=2;" +
      "#<OUT>.MAP_UDF_RES_P_P_RETAILPRICE_1#=0.5;" +
      "#<OUT>.MAP_UDF_RES_P_P_COMMENT_1#=\"comment\";"

    actual.concatenated should be(expectedUDF)
  }

  case class NestedNested(second: Nested, partPrice: PartPrice)

  "MapUDFGenerator" should "flatten a double nested Complex output" in {

    val ast = typecheck(reify {
      () => (p: Part) =>
        NestedNested(Nested(Part(0, "o", 2, 0.5, "comment")), PartPrice(1, p.p_retailprice * p.p_size))
    }.tree)


    val actual = new MapUDFGenerator(ast, symbolTable).generate

    val expectedUDF = "#<OUT>.MAP_UDF_RES_SECOND_P_P_PARTKEY_1#=0;" +
      "#<OUT>.MAP_UDF_RES_SECOND_P_P_NAME_1#=\"o\";" +
      "#<OUT>.MAP_UDF_RES_SECOND_P_P_SIZE_1#=2;" +
      "#<OUT>.MAP_UDF_RES_SECOND_P_P_RETAILPRICE_1#=0.5;" +
      "#<OUT>.MAP_UDF_RES_SECOND_P_P_COMMENT_1#=\"comment\";" +
      "#<OUT>.MAP_UDF_RES_PARTPRICE_PART_PRICE_KEY_1#=1;" +
      "#<OUT>.MAP_UDF_RES_PARTPRICE_PART_PRICE_AMOUNT_1#=(#PART.P_RETAILPRICE#*#PART.P_SIZE#);"

    actual.concatenated should be(expectedUDF)
  }

  case class NestedInOut(nested2: Nested, partPrice2: PartPrice)

  "MapUDFGenerator" should "flatten a double nested input if it is returned as a Complex output" in {

    val ast = typecheck(reify {
      () => (nestedNested: NestedInOut) => nestedNested
    }.tree)

    val symTbl = Map[String, String]("nestedNested" -> "NESTED")
    val actual = new MapUDFGenerator(ast, symTbl).generate

    val expectedUDF = "#<OUT>.MAP_UDF_RES_NESTED2_P_P_PARTKEY_1#=#NESTED.NESTED2_P_P_PARTKEY#;" +
      "#<OUT>.MAP_UDF_RES_NESTED2_P_P_NAME_1#=#NESTED.NESTED2_P_P_NAME#;" +
      "#<OUT>.MAP_UDF_RES_NESTED2_P_P_SIZE_1#=#NESTED.NESTED2_P_P_SIZE#;" +
      "#<OUT>.MAP_UDF_RES_NESTED2_P_P_RETAILPRICE_1#=#NESTED.NESTED2_P_P_RETAILPRICE#;" +
      "#<OUT>.MAP_UDF_RES_NESTED2_P_P_COMMENT_1#=#NESTED.NESTED2_P_P_COMMENT#;" +
      "#<OUT>.MAP_UDF_RES_PARTPRICE2_PART_PRICE_KEY_1#=#NESTED.PARTPRICE2_PART_PRICE_KEY#;" +
      "#<OUT>.MAP_UDF_RES_PARTPRICE2_PART_PRICE_AMOUNT_1#=#NESTED.PARTPRICE2_PART_PRICE_AMOUNT#;"

    actual.concatenated should be(expectedUDF)
  }

  "MapUDFGenerator" should "flatten tuple with nested type" in {

    val ast = typecheck(reify {
      () => (p: Part) =>
        (p.p_partkey,
          NestedNested(Nested(Part(0, "o", 2, 0.5, "comment")), PartPrice(1, p.p_retailprice * p.p_size)),
          3.5)
    }.tree)

    val actual = new MapUDFGenerator(ast, symbolTable).generate

    val expResIdentifier1 = "MAP_UDF_RES__1_1"
    val expResIdentifier2 = "MAP_UDF_RES__2_SECOND_P_P_PARTKEY_1"
    val expResIdentifier3 = "MAP_UDF_RES__2_SECOND_P_P_NAME_1"
    val expResIdentifier4 = "MAP_UDF_RES__2_SECOND_P_P_SIZE_1"
    val expResIdentifier5 = "MAP_UDF_RES__2_SECOND_P_P_RETAILPRICE_1"
    val expResIdentifier6 = "MAP_UDF_RES__2_SECOND_P_P_COMMENT_1"
    val expResIdentifier7 = "MAP_UDF_RES__2_PARTPRICE_PART_PRICE_KEY_1"
    val expResIdentifier8 = "MAP_UDF_RES__2_PARTPRICE_PART_PRICE_AMOUNT_1"
    val expResIdentifier9 = "MAP_UDF_RES__3_1"

    val expectedUDF = s"#<OUT>.$expResIdentifier1#=#PART.P_PARTKEY#;" +
      s"#<OUT>.$expResIdentifier2#=0;" +
      "#<OUT>.MAP_UDF_RES__2_SECOND_P_P_NAME_1#=\"o\";" +
      s"#<OUT>.$expResIdentifier4#=2;" +
      s"#<OUT>.$expResIdentifier5#=0.5;" +
      "#<OUT>.MAP_UDF_RES__2_SECOND_P_P_COMMENT_1#=\"comment\";" +
      s"#<OUT>.$expResIdentifier7#=1;" +
      s"#<OUT>.$expResIdentifier8#=(#PART.P_RETAILPRICE#*#PART.P_SIZE#);" +
      s"#<OUT>.$expResIdentifier9#=3.5;"

    val expectedOutputs = Seq(
      MapUdfOutAttr("INT", expResIdentifier1, expResIdentifier1),
      MapUdfOutAttr("INT", expResIdentifier2, expResIdentifier2),
      MapUdfOutAttr("VARCHAR", expResIdentifier3, expResIdentifier3),
      MapUdfOutAttr("INT", expResIdentifier4, expResIdentifier4),
      MapUdfOutAttr("DOUBLE", expResIdentifier5, expResIdentifier5),
      MapUdfOutAttr("VARCHAR", expResIdentifier6, expResIdentifier6),
      MapUdfOutAttr("INT", expResIdentifier7, expResIdentifier7),
      MapUdfOutAttr("DOUBLE", expResIdentifier8, expResIdentifier8),
      MapUdfOutAttr("DOUBLE", expResIdentifier9, expResIdentifier9)
    )

    actual.concatenated should be(expectedUDF)
    actual.mapUdfOutAttr.size should be(9)
    for (expected <- expectedOutputs) actual.mapUdfOutAttr should contain(expected)

    actual.concatenated should be(expectedUDF)
  }

  "MapUDFGenerator" should "consider point before line calculation" in {

    val ast = typecheck(reify {
      () => (p: Part) => p.p_retailprice + p.p_size * p.p_partkey
    }.tree)

    val actual = new MapUDFGenerator(ast, symbolTable).generate

    val expectedUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=" +
      "(#PART.P_RETAILPRICE#+(#PART.P_SIZE#*#PART.P_PARTKEY#));"

    actual.concatenated should be(expectedUDF)
    actual.mapUdfOutAttr.size should be(1)
  }

  "MapUDFGenerator" should "correctly compile local val as output" in {

    val ast = typecheck(reify {
      () => (p: Part) => {
        val x = 2.5
        x
      }
    }.tree)

    val actual = new MapUDFGenerator(ast, symbolTable).generate

    val expectedUDF = s"double x=2.5;" +
      s"#<OUT>.$nextExpectedOutputIdentifier#=x;"

    actual.concatenated should be(expectedUDF)
    actual.mapUdfOutAttr.size should be(1)
  }


  case class IntWrapper(toDouble: Int)

  "MapUDFGenerator" should "consider toDouble as a value instead of a method" in {

    val ast = typecheck(reify {
      () => (input: IntWrapper) => input.toDouble * 0.5
    }.tree)

    val actual = new MapUDFGenerator(ast, Map[String, String]("input" -> "TABLE")).generate
    val expectedUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=(#TABLE.TODOUBLE#*0.5);"

    actual.concatenated should be(expectedUDF)
  }

  case class NestedPart(triplet: (Int, Part, Double))

  "MapUDFGenerator" should "compile projection of nested input" in {

    val ast = typecheck(reify {
      () => (input: NestedPart) => input.triplet
    }.tree)

    val actual = new MapUDFGenerator(ast, Map[String, String]("input" -> "NESTEDPART")).generate
    val expectedUDF = s"#<OUT>.MAP_UDF_RES_TRIPLET_1_1#=#NESTEDPART.TRIPLET_1#;" +
      s"#<OUT>.MAP_UDF_RES_TRIPLET_2_P_PARTKEY_1#=#NESTEDPART.TRIPLET_2_P_PARTKEY#;" +
      s"#<OUT>.MAP_UDF_RES_TRIPLET_2_P_NAME_1#=#NESTEDPART.TRIPLET_2_P_NAME#;" +
      s"#<OUT>.MAP_UDF_RES_TRIPLET_2_P_SIZE_1#=#NESTEDPART.TRIPLET_2_P_SIZE#;" +
      s"#<OUT>.MAP_UDF_RES_TRIPLET_2_P_RETAILPRICE_1#=#NESTEDPART.TRIPLET_2_P_RETAILPRICE#;" +
      s"#<OUT>.MAP_UDF_RES_TRIPLET_2_P_COMMENT_1#=#NESTEDPART.TRIPLET_2_P_COMMENT#;" +
      s"#<OUT>.MAP_UDF_RES_TRIPLET_3_1#=#NESTEDPART.TRIPLET_3#;"

    actual.concatenated should be(expectedUDF)
  }

  private def typecheck(ast: Tree): Tree = tb.typecheck(ast)

}

object MapUDFGeneratorTest {

  var currentExpectedOutputId = 0
  var currentExpectedLocalVarId = 0

  def nextExpectedOutputIdentifier = {
    currentExpectedOutputId = currentExpectedOutputId + 1
    s"MAP_UDF_RES_$currentExpectedOutputId"
  }

  def nextExpectedLocalVarIdentifier = {
    currentExpectedLocalVarId = currentExpectedLocalVarId + 1
    s"map_udf_local_var_$currentExpectedOutputId"
  }

  case class Part(p_partkey: Int, p_name: String, p_size: Int, p_retailprice: Double, p_comment: String)

  case class PartPrice(part_price_key: Int, part_price_amount: Double)

  case class Nested(p: Part)

  class ArbitraryClass(var value: Int)

}
