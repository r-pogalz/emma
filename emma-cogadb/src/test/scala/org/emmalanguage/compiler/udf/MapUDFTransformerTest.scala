package org.emmalanguage
package compiler.udf

import eu.stratosphere.emma.macros._
import org.emmalanguage.compiler.RuntimeCompiler
import org.emmalanguage.compiler.lang.cogadb.ast.{MapUdfOutAttr, _}
import org.emmalanguage.compiler.udf.MapUDFTransformerTest._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.language.implicitConversions
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

@RunWith(classOf[JUnitRunner])
class MapUDFTransformerTest extends FlatSpec with Matchers with BeforeAndAfter {

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  val symbolTable = Map[String, String](
    "l.lineNumber" -> "LINEITEM.L_LINENUMBER",
    "l.quantity" -> "LINEITEM.L_QUANTITY",
    "l.extendedPrice" -> "LINEITEM.L_EXTENDEDPRICE",
    "l.discount" -> "LINEITEM.L_DISCOUNT",
    "l.tax" -> "LINEITEM.L_TAX"
  )

  //  val compiler = new RuntimeCompiler()
  //  import compiler._
  //
  //  val idPipeline: u.Expr[Any] => u.Tree =
  //    compiler
  //    .identity(typeCheck = true)
  //    .compose(_.tree)
  //
  //  "MapUDFTransformer" should "for testing" in {
  //    val ast: u.Tree = idPipeline(u.reify {
  //      def fun(l: Lineitem): Double = {
  //        val t: Double = 0.5
  //        l.extendedPrice + t
  //      }
  //    })
  //
  //    println(ast)
  //  }

  implicit def MapUdfWrapper(t: MapUdf) = new MapUdfHelperClass(t)

  final class MapUdfHelperClass(udf: MapUdf) {
    implicit def concatenate: String = udf.mapUdfCode.map(_.code).mkString
  }

  "MapUDFTransformer" should "compile final if-then-else with single then and else statement" in {
    val ast = typecheck(reify {
      (() => ((l: Lineitem) => if (l.quantity > 5) l.extendedPrice else l.discount))
    }.tree)

    val actual = new MapUDFTransformer(ast, symbolTable).transform

    val expectedOutputIde = nextExpectedOutputIdentifier
    val expectedLocalVar = nextExpectedLocalVarIdentifier
    val expectedUDF = s"double $expectedLocalVar;" +
      s"if((#LINEITEM.L_QUANTITY#>5)){" +
      s"$expectedLocalVar=#LINEITEM.L_EXTENDEDPRICE#;" +
      s"}else{" +
      s"$expectedLocalVar=#LINEITEM.L_DISCOUNT#;" +
      s"}" +
      s"#<OUT>.$expectedOutputIde#=$expectedLocalVar;"

    actual.concatenate should be(expectedUDF)
  }

  //TODO: test missing input mappings, filter, blocks

  "MapUDFTransformer" should "compile a UDF with a single basic type as input" in {

    val astWithShort = typecheck(reify {
      (() => ((s: Short) => s))
    }.tree)
    val actualShortUDF = new MapUDFTransformer(astWithShort, Map[String, String]("s" -> "SHORT.VAL")).
                         transform
    val expectedShortUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#SHORT.VAL#;"
    actualShortUDF.concatenate should be(expectedShortUDF)
    actualShortUDF.mapUdfOutAttr.size should be(1)

    val astWithInt = typecheck(reify {
      (() => ((i: Int) => i))
    }.tree)
    val actualIntUDF = new MapUDFTransformer(astWithInt, Map[String, String]("i" -> "INT.VAL")).transform
    val expectedIntUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#INT.VAL#;"
    actualIntUDF.concatenate should be(expectedIntUDF)
    actualIntUDF.mapUdfOutAttr.size should be(1)

    val astWithLong = typecheck(reify {
      (() => ((l: Long) => l))
    }.tree)
    val actualLongUDF = new MapUDFTransformer(astWithLong, Map[String, String]("l" -> "LONG.VAL")).transform
    val expectedLongUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#LONG.VAL#;"
    actualLongUDF.concatenate should be(expectedLongUDF)
    actualLongUDF.mapUdfOutAttr.size should be(1)

    val astWithFloat = typecheck(reify {
      (() => ((f: Float) => f))
    }.tree)
    val actualFloatUDF = new MapUDFTransformer(astWithFloat, Map[String, String]("f" -> "FLOAT.VAL")).transform
    val expectedFloatUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#FLOAT.VAL#;"
    actualFloatUDF.concatenate should be(expectedFloatUDF)
    actualFloatUDF.mapUdfOutAttr.size should be(1)

    val astWithDouble = typecheck(reify {
      (() => ((d: Double) => d))
    }.tree)
    val actualDoubleUDF = new MapUDFTransformer(astWithDouble, Map[String, String]("d" -> "DOUBLE.VAL")).transform
    val expectedDoubleUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#DOUBLE.VAL#;"
    actualDoubleUDF.concatenate should be(expectedDoubleUDF)
    actualDoubleUDF.mapUdfOutAttr.size should be(1)

    val astWithBoolean = typecheck(reify {
      (() => ((b: Boolean) => b))
    }.tree)
    val actualBoolUDF = new MapUDFTransformer(astWithBoolean, Map[String, String]("b" -> "BOOL.VAL")).transform
    val expectedBoolUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#BOOL.VAL#;"
    actualBoolUDF.concatenate should be(expectedBoolUDF)
    actualBoolUDF.mapUdfOutAttr.size should be(1)

    val astWithChar = typecheck(reify {
      (() => ((c: Char) => c))
    }.tree)
    val actualCharUDF = new MapUDFTransformer(astWithChar, Map[String, String]("c" -> "CHAR.VAL")).transform
    val expectedCharUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#CHAR.VAL#;"
    actualCharUDF.concatenate should be(expectedCharUDF)
    actualCharUDF.mapUdfOutAttr.size should be(1)

    val astWithString = typecheck(reify {
      (() => ((str: String) => str))
    }.tree)
    val actualStringUDF = new MapUDFTransformer(astWithString, Map[String, String]("str" -> "VARCHAR.VAL")).transform
    val expectedStringUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#VARCHAR.VAL#;"
    actualStringUDF.concatenate should be(expectedStringUDF)
    actualStringUDF.mapUdfOutAttr.size should be(1)

  }

  "MapUDFTransformer" should "compile a UDF with a Tuple input containing basic types" in {

    val ast = typecheck(reify {
      (() => ((input: (Int, Double, String)) => input._1 + input._2))
    }.tree)

    val symTbl = Map[String, String]("input._1" -> "TRIPLE.VAL1", "input._2" -> "TRIPLE.VAL2")
    val actual = new MapUDFTransformer(ast, symTbl).transform

    val expectedUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=(#TRIPLE.VAL1#+#TRIPLE.VAL2#);"

    actual.concatenate should be(expectedUDF)
    actual.mapUdfOutAttr.size should be(1)
  }

  "MapUDFTransformer" should "compile a UDF with a Tuple input containing case classes and basic types" in {

    val ast = typecheck(reify {
      (() => ((input: (Lineitem, Double, String)) => input._1.extendedPrice + input._2))
    }.tree)

    val symTbl = Map[String, String]("input._1.extendedPrice" -> "LINEITEM.EXTENDEDPRICE",
      "input._2" -> "TRIPLE.VAL2")
    val actual = new MapUDFTransformer(ast, symTbl).transform

    val expectedUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=(#LINEITEM.EXTENDEDPRICE#+#TRIPLE.VAL2#);"

    actual.concatenate should be(expectedUDF)
    actual.mapUdfOutAttr.size should be(1)
  }

  ignore should "throw an exception for missing input mappings" in {

    val ast = typecheck(reify {
      (() => ((l: Lineitem) => l.lineNumber + l.suppKey))
    }.tree)

    the[IllegalArgumentException] thrownBy {
      val actual = new MapUDFTransformer(ast, Map[String, String]()).transform
    } should have message "No mapping found for [l.suppKey]"
  }

  ignore should "throw an exception if a UDF parameter type is an arbitrary class" in {

    val ast = typecheck(reify {
      (() => ((input: ArbitraryClass) => input.value))
    }.tree)

    the[IllegalArgumentException] thrownBy {
      val actual = new MapUDFTransformer(ast, symbolTable).transform
    } should have message "ArbitraryClass is not a case class."
  }

  ignore should "throw an exception if a UDF parameter type is a nested case class" in {

    val ast = typecheck(reify {
      (() => ((input: Nested) => input.l.partKey))
    }.tree)

    the[IllegalArgumentException] thrownBy {
      val actual = new MapUDFTransformer(ast, symbolTable).transform
    } should have message "Nested is a nested case class."
  }

  ignore should "throw an exception if a UDF parameter type in a Tuple is an arbitrary class" in {

    val ast = typecheck(reify {
      (() => ((input: (Int, ArbitraryClass)) => input._2.value))
    }.tree)

    the[IllegalArgumentException] thrownBy {
      val actual = new MapUDFTransformer(ast, symbolTable).transform
    } should have message "ArbitraryClass is not a case class."
  }

  "MapUDFTransformer" should "return correct result type" in {

    val ast = typecheck(reify {
      (() => ((l: Lineitem) => l.quantity * l.extendedPrice))
    }.tree)

    val actual = new MapUDFTransformer(ast, symbolTable).transform

    val expResIdentifier = nextExpectedOutputIdentifier
    val expectedUDF = s"#<OUT>.$expResIdentifier#=(#LINEITEM.L_QUANTITY#*#LINEITEM.L_EXTENDEDPRICE#);"

    actual.concatenate should be(expectedUDF)
    actual.mapUdfOutAttr.size should be(1)
    actual.mapUdfOutAttr.head should be(MapUdfOutAttr("DOUBLE", expResIdentifier, expResIdentifier))
  }

  "MapUDFTransformer" should "return multiple results for a Tuple output type" in {

    val ast = typecheck(reify {
      (() => ((l: Lineitem) => (l.lineNumber, l.extendedPrice, l.quantity)))
    }.tree)

    val actual = new MapUDFTransformer(ast, symbolTable).transform

    val expResIdentifier1 = nextExpectedOutputIdentifier
    val expResIdentifier2 = nextExpectedOutputIdentifier
    val expResIdentifier3 = nextExpectedOutputIdentifier
    val expectedUDF = s"#<OUT>.$expResIdentifier1#=#LINEITEM.L_LINENUMBER#;" +
      s"#<OUT>.$expResIdentifier2#=#LINEITEM.L_EXTENDEDPRICE#;" +
      s"#<OUT>.$expResIdentifier3#=#LINEITEM.L_QUANTITY#;"
    val expectedOutputs = Seq(
      MapUdfOutAttr("INT", expResIdentifier1, expResIdentifier1),
      MapUdfOutAttr("DOUBLE", expResIdentifier2, expResIdentifier2),
      MapUdfOutAttr("INT", expResIdentifier3, expResIdentifier3)
    )

    actual.concatenate should be(expectedUDF)
    actual.mapUdfOutAttr.size should be(3)
    for (expected <- expectedOutputs) actual.mapUdfOutAttr should contain(expected)
  }

  "MapUDFTransformer" should "return multiple results for a Complex output type" in {

    val ast = typecheck(reify {
      (() => ((l: Lineitem) => DiscPrice(l.lineNumber, l.extendedPrice * (1 - l.discount))))
    }.tree)

    val actual = new MapUDFTransformer(ast, symbolTable).transform

    val expResIdentifier1 = nextExpectedOutputIdentifier
    val expResIdentifier2 = nextExpectedOutputIdentifier
    val expectedUDF = s"#<OUT>.$expResIdentifier1#=#LINEITEM.L_LINENUMBER#;" +
      s"#<OUT>.$expResIdentifier2#=(#LINEITEM.L_EXTENDEDPRICE#*(1-#LINEITEM.L_DISCOUNT#));"
    val expectedOutputs = Seq(
      MapUdfOutAttr("INT", expResIdentifier1, expResIdentifier1),
      MapUdfOutAttr("DOUBLE", expResIdentifier2, expResIdentifier2))

    actual.concatenate should be(expectedUDF)
    actual.mapUdfOutAttr.size should be(2)
    for (expected <- expectedOutputs) actual.mapUdfOutAttr should contain(expected)
  }

  "MapUDFTransformer" should "consider point before line calculation" in {

    val ast = typecheck(reify {
      (() => ((l: Lineitem) => l.extendedPrice + l.tax * l.discount))
    }.tree)

    val actual = new MapUDFTransformer(ast, symbolTable).transform

    val expectedUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=" +
      "(#LINEITEM.L_EXTENDEDPRICE#+(#LINEITEM.L_TAX#*#LINEITEM.L_DISCOUNT#));"

    actual.concatenate should be(expectedUDF)
    actual.mapUdfOutAttr.size should be(1)
  }

  private def typecheck(ast: Tree): Tree = tb.typecheck(ast)

}

object MapUDFTransformerTest {

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

  case class Lineitem(orderKey: Int,
    partKey: Int,
    suppKey: Int,
    lineNumber: Int,
    quantity: Int,
    extendedPrice: Double,
    discount: Double,
    tax: Double,
    returnFlag: Char,
    lineStatus: Char,
    shipDate: String,
    commitDate: String,
    receiptDate: String,
    shipInstruct: String,
    shipMode: String,
    comment: String)

  case class DiscPrice(linenumber: Int, amount: Double)

  case class Nested(l: Lineitem)

  class ArbitraryClass(var value: Int)

}
