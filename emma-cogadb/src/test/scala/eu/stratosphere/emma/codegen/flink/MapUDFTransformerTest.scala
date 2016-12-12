package eu.stratosphere.emma.codegen.cogadb

import eu.stratosphere.emma.codegen.cogadb.MapUDFTransformerTest.{ArbitraryClass, DiscPrice, Lineitem, Nested, _}
import eu.stratosphere.emma.codegen.cogadb.udfcompilation.MapUDFTransformer
import eu.stratosphere.emma.codegen.cogadb.udfcompilation.common._
import eu.stratosphere.emma.macros._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

@RunWith(classOf[JUnitRunner])
class MapUDFTransformerTest extends FlatSpec with Matchers with BeforeAndAfter with RuntimeUtil {

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  val mapUdfTransformer = new MapUDFTransformer

  val symbolTable = Map[String, String](
    "l.lineNumber" -> "LINEITEM.L_LINENUMBER",
    "l.quantity" -> "LINEITEM.L_QUANTITY",
    "l.extendedPrice" -> "LINEITEM.L_EXTENDEDPRICE",
    "l.discount" -> "LINEITEM.L_DISCOUNT",
    "l.tax" -> "LINEITEM.L_TAX"
  )

//  "MapUDFTransformer" should "for testing" in {
//    val ast = typecheck(reify {
//      def fun(l: Lineitem): Double = if(l.quantity > 5) 1.5 else 3.0
//    }.tree)
//
//    val actual = mapUdfTransformer.transform(ast, symbolTable)
//    println(actual.udf.mkString)
//  }

  "MapUDFTransformer" should "compile final if-then-else with single then and else statement" in {
    val ast = typecheck(reify {
      def fun(l: Lineitem): Double = if(l.quantity > 5) l.extendedPrice else l.discount
    }.tree)

    val actual = mapUdfTransformer.transform(ast, symbolTable)
    
    val expectedOutputIde = nextExpectedOutputIdentifier
    val expectedLocalVar = nextExpectedLocalVarIdentifier
    val expectedUDF = s"double $expectedLocalVar;" +
      s"if((#LINEITEM.L_QUANTITY#>5)){" +
      s"$expectedLocalVar=#LINEITEM.L_EXTENDEDPRICE#;" +
      s"}else{" +
      s"$expectedLocalVar=#LINEITEM.L_DISCOUNT#;" +
      s"}" +
      s"#<OUT>.$expectedOutputIde#=$expectedLocalVar;"
    
    actual.udf.mkString should be(expectedUDF)
  }

  //TODO: test missing input mappings, filter, blocks

  "MapUDFTransformer" should "compile a UDF with a single basic type as input" in {

    val astWithShort = typecheck(reify {
      def fun(s: Short) = s
    }.tree)
    val actualShortUDF = mapUdfTransformer.transform(astWithShort, Map[String, String]("s" -> "SHORT.VAL"))
    val expectedShortUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#SHORT.VAL#;"
    actualShortUDF.udf.mkString should be(expectedShortUDF)
    actualShortUDF.output.size should be(1)

    val astWithInt = typecheck(reify {
      def fun(i: Int) = i
    }.tree)
    val actualIntUDF = mapUdfTransformer.transform(astWithInt, Map[String, String]("i" -> "INT.VAL"))
    val expectedIntUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#INT.VAL#;"
    actualIntUDF.udf.mkString should be(expectedIntUDF)
    actualIntUDF.output.size should be(1)

    val astWithLong = typecheck(reify {
      def fun(l: Long) = l
    }.tree)
    val actualLongUDF = mapUdfTransformer.transform(astWithLong, Map[String, String]("l" -> "LONG.VAL"))
    val expectedLongUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#LONG.VAL#;"
    actualLongUDF.udf.mkString should be(expectedLongUDF)
    actualLongUDF.output.size should be(1)

    val astWithFloat = typecheck(reify {
      def fun(f: Float) = f
    }.tree)
    val actualFloatUDF = mapUdfTransformer.transform(astWithFloat, Map[String, String]("f" -> "FLOAT.VAL"))
    val expectedFloatUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#FLOAT.VAL#;"
    actualFloatUDF.udf.mkString should be(expectedFloatUDF)
    actualFloatUDF.output.size should be(1)

    val astWithDouble = typecheck(reify {
      def fun(d: Double) = d
    }.tree)
    val actualDoubleUDF = mapUdfTransformer.transform(astWithDouble, Map[String, String]("d" -> "DOUBLE.VAL"))
    val expectedDoubleUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#DOUBLE.VAL#;"
    actualDoubleUDF.udf.mkString should be(expectedDoubleUDF)
    actualDoubleUDF.output.size should be(1)

    val astWithBoolean = typecheck(reify {
      def fun(b: Boolean) = b
    }.tree)
    val actualBoolUDF = mapUdfTransformer.transform(astWithBoolean, Map[String, String]("b" -> "BOOL.VAL"))
    val expectedBoolUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#BOOL.VAL#;"
    actualBoolUDF.udf.mkString should be(expectedBoolUDF)
    actualBoolUDF.output.size should be(1)

    val astWithChar = typecheck(reify {
      def fun(c: Char) = c
    }.tree)
    val actualCharUDF = mapUdfTransformer.transform(astWithChar, Map[String, String]("c" -> "CHAR.VAL"))
    val expectedCharUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#CHAR.VAL#;"
    actualCharUDF.udf.mkString should be(expectedCharUDF)
    actualCharUDF.output.size should be(1)

    val astWithString = typecheck(reify {
      def fun(str: String) = str
    }.tree)
    val actualStringUDF = mapUdfTransformer.transform(astWithString, Map[String, String]("str" -> "VARCHAR.VAL"))
    val expectedStringUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#VARCHAR.VAL#;"
    actualStringUDF.udf.mkString should be(expectedStringUDF)
    actualStringUDF.output.size should be(1)

  }

  "MapUDFTransformer" should "compile a UDF with a Tuple input containing basic types" in {

    val ast = typecheck(reify {
      def fun(input: (Int, Double, String)) = input._1 + input._2
    }.tree)

    val symTbl = Map[String, String]("input._1" -> "TRIPLE.VAL1", "input._2" -> "TRIPLE.VAL2")
    val actual = mapUdfTransformer.transform(ast, symTbl)

    val expectedUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=(#TRIPLE.VAL1#+#TRIPLE.VAL2#);"

    actual.udf.mkString should be(expectedUDF)
    actual.output.size should be(1)
  }

  "MapUDFTransformer" should "compile a UDF with a Tuple input containing case classes and basic types" in {

    val ast = typecheck(reify {
      def fun(input: (Lineitem, Double, String)) = input._1.extendedPrice + input._2
    }.tree)

    val symTbl = Map[String, String]("input._1.extendedPrice" -> "LINEITEM.EXTENDEDPRICE",
      "input._2" -> "TRIPLE.VAL2")
    val actual = mapUdfTransformer.transform(ast, symTbl)

    val expectedUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=(#LINEITEM.EXTENDEDPRICE#+#TRIPLE.VAL2#);"

    actual.udf.mkString should be(expectedUDF)
    actual.output.size should be(1)
  }

  ignore should "throw an exception for missing input mappings" in {

    val ast = typecheck(reify {
      def fun(l: Lineitem) = l.lineNumber + l.suppKey
    }.tree)

    the[IllegalArgumentException] thrownBy {
      val actual = mapUdfTransformer.transform(ast, Map[String, String]())
    } should have message "No mapping found for [l.suppKey]"
  }

  ignore should "throw an exception if a UDF parameter type is an arbitrary class" in {

    val ast = typecheck(reify {
      def fun(input: ArbitraryClass) = input.value
    }.tree)

    the[IllegalArgumentException] thrownBy {
      val actual = mapUdfTransformer.transform(ast, symbolTable)
    } should have message "ArbitraryClass is not a case class."
  }

  ignore should "throw an exception if a UDF parameter type is a nested case class" in {

    val ast = typecheck(reify {
      def fun(input: Nested) = input.l.partKey
    }.tree)

    the[IllegalArgumentException] thrownBy {
      val actual = mapUdfTransformer.transform(ast, symbolTable)
    } should have message "Nested is a nested case class."
  }

  ignore should "throw an exception if a UDF parameter type in a Tuple is an arbitrary class" in {

    val ast = typecheck(reify {
      def fun(input: (Int, ArbitraryClass)) = input._2.value
    }.tree)

    the[IllegalArgumentException] thrownBy {
      val actual = mapUdfTransformer.transform(ast, symbolTable)
    } should have message "ArbitraryClass is not a case class."
  }

  "MapUDFTransformer" should "return correct result type" in {

    val ast = typecheck(reify {
      def fun(l: Lineitem) = l.quantity * l.extendedPrice
    }.tree)

    val actual = mapUdfTransformer.transform(ast, symbolTable)

    val expResIdentifier = nextExpectedOutputIdentifier
    val expectedUDF = s"#<OUT>.$expResIdentifier#=(#LINEITEM.L_QUANTITY#*#LINEITEM.L_EXTENDEDPRICE#);"

    actual.udf.mkString should be(expectedUDF)
    actual.output.size should be(1)
    actual.output.head should be(UDFOutput(TypeName(expResIdentifier), typeOf[Double]))
  }

  "MapUDFTransformer" should "return multiple results for a Tuple output type" in {

    val ast = typecheck(reify {
      def fun(l: Lineitem) = (l.lineNumber, l.extendedPrice, l.quantity)
    }.tree)

    val actual = mapUdfTransformer.transform(ast, symbolTable)

    val expResIdentifier1 = nextExpectedOutputIdentifier
    val expResIdentifier2 = nextExpectedOutputIdentifier
    val expResIdentifier3 = nextExpectedOutputIdentifier
    val expectedUDF = s"#<OUT>.$expResIdentifier1#=#LINEITEM.L_LINENUMBER#;" +
      s"#<OUT>.$expResIdentifier2#=#LINEITEM.L_EXTENDEDPRICE#;" +
      s"#<OUT>.$expResIdentifier3#=#LINEITEM.L_QUANTITY#;"
    val expectedOutputs = Seq(
      UDFOutput(TypeName(expResIdentifier1), typeOf[Int]),
      UDFOutput(TypeName(expResIdentifier2), typeOf[Double]),
      UDFOutput(TypeName(expResIdentifier3), typeOf[Int])
    )

    actual.udf.mkString should be(expectedUDF)
    actual.output.size should be(3)
    for (expected <- expectedOutputs) actual.output should contain(expected)
  }

  "MapUDFTransformer" should "return multiple results for a Complex output type" in {

    val ast = typecheck(reify {
      def fun(l: Lineitem) = DiscPrice(l.lineNumber, l.extendedPrice * (1 - l.discount))
    }.tree)

    val actual = mapUdfTransformer.transform(ast, symbolTable)

    val expResIdentifier1 = nextExpectedOutputIdentifier
    val expResIdentifier2 = nextExpectedOutputIdentifier
    val expectedUDF = s"#<OUT>.$expResIdentifier1#=#LINEITEM.L_LINENUMBER#;" +
      s"#<OUT>.$expResIdentifier2#=(#LINEITEM.L_EXTENDEDPRICE#*(1-#LINEITEM.L_DISCOUNT#));"
    val expectedOutputs = Seq(
      UDFOutput(TypeName(expResIdentifier1), typeOf[Int]),
      UDFOutput(TypeName(expResIdentifier2), typeOf[Double]))

    actual.udf.mkString should be(expectedUDF)
    actual.output.size should be(2)
    for (expected <- expectedOutputs) actual.output should contain(expected)
  }

  "MapUDFTransformer" should "consider point before line calculation" in {

    val ast = typecheck(reify {
      def fun(l: Lineitem) = l.extendedPrice + l.tax * l.discount
    }.tree)

    val actual = mapUdfTransformer.transform(ast, symbolTable)

    val expectedUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=" +
      "(#LINEITEM.L_EXTENDEDPRICE#+(#LINEITEM.L_TAX#*#LINEITEM.L_DISCOUNT#));"

    actual.udf.mkString should be(expectedUDF)
    actual.output.size should be(1)
  }

  private def typecheck(ast: Tree): Tree = tb.typecheck(ast).children.head

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
