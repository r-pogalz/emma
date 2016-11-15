package eu.stratosphere.emma.codegen.cogadb

import eu.stratosphere.emma.codegen.cogadb.UDFCodeGenerator._
import eu.stratosphere.emma.codegen.cogadb.UDFCodeGeneratorTest.{ArbitraryClass, DiscPrice, Lineitem, Nested}
import eu.stratosphere.emma.codegen.cogadb.UDFCodeGeneratorTest._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

@RunWith(classOf[JUnitRunner])
class UDFCodeGeneratorTest extends FlatSpec with Matchers with BeforeAndAfter {

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  val udfClosure = new UDFClosure()

  before {
    udfClosure.symbolTable += "l.lineNumber" -> "LINEITEM.L_LINENUMBER"
    udfClosure.symbolTable += "l.quantity" -> "LINEITEM.L_QUANTITY"
    udfClosure.symbolTable += "l.extendedPrice" -> "LINEITEM.L_EXTENDEDPRICE"
    udfClosure.symbolTable += "l.discount" -> "LINEITEM.L_DISCOUNT"
    udfClosure.symbolTable += "l.tax" -> "LINEITEM.L_TAX"
  }

  //TODO: test missing input mappings, filter, blocks

  "UDFCodeGenerator" should "compile a UDF with a single basic type as input" in {

    val astWithShort = typecheck(reify {
      def fun(s: Short) = s
    }.tree)
    udfClosure.symbolTable += "s" -> "SHORT.VAL"
    val actualShortUDF = UDFCodeGenerator.generateFor(MapUDF(astWithShort), udfClosure)
    val expectedShortUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#SHORT.VAL#;"
    actualShortUDF.udf should be(expectedShortUDF)
    actualShortUDF.output.size should be(1)

    val astWithInt = typecheck(reify {
      def fun(i: Int) = i
    }.tree)
    udfClosure.symbolTable += "i" -> "INT.VAL"
    val actualIntUDF = UDFCodeGenerator.generateFor(MapUDF(astWithInt), udfClosure)
    val expectedIntUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#INT.VAL#;"
    actualIntUDF.udf should be(expectedIntUDF)
    actualIntUDF.output.size should be(1)

    val astWithLong = typecheck(reify {
      def fun(l: Long) = l
    }.tree)
    udfClosure.symbolTable += "l" -> "LONG.VAL"
    val actualLongUDF = UDFCodeGenerator.generateFor(MapUDF(astWithLong), udfClosure)
    val expectedLongUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#LONG.VAL#;"
    actualLongUDF.udf should be(expectedLongUDF)
    actualLongUDF.output.size should be(1)

    val astWithFloat = typecheck(reify {
      def fun(f: Float) = f
    }.tree)
    udfClosure.symbolTable += "f" -> "FLOAT.VAL"
    val actualFloatUDF = UDFCodeGenerator.generateFor(MapUDF(astWithFloat), udfClosure)
    val expectedFloatUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#FLOAT.VAL#;"
    actualFloatUDF.udf should be(expectedFloatUDF)
    actualFloatUDF.output.size should be(1)

    val astWithDouble = typecheck(reify {
      def fun(d: Double) = d
    }.tree)
    udfClosure.symbolTable += "d" -> "DOUBLE.VAL"
    val actualDoubleUDF = UDFCodeGenerator.generateFor(MapUDF(astWithDouble), udfClosure)
    val expectedDoubleUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#DOUBLE.VAL#;"
    actualDoubleUDF.udf should be(expectedDoubleUDF)
    actualDoubleUDF.output.size should be(1)

    val astWithBoolean = typecheck(reify {
      def fun(b: Boolean) = b
    }.tree)
    udfClosure.symbolTable += "b" -> "BOOL.VAL"
    val actualBoolUDF = UDFCodeGenerator.generateFor(MapUDF(astWithBoolean), udfClosure)
    val expectedBoolUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#BOOL.VAL#;"
    actualBoolUDF.udf should be(expectedBoolUDF)
    actualBoolUDF.output.size should be(1)

    val astWithChar = typecheck(reify {
      def fun(c: Char) = c
    }.tree)
    udfClosure.symbolTable += "c" -> "CHAR.VAL"
    val actualCharUDF = UDFCodeGenerator.generateFor(MapUDF(astWithChar), udfClosure)
    val expectedCharUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#CHAR.VAL#;"
    actualCharUDF.udf should be(expectedCharUDF)
    actualCharUDF.output.size should be(1)

    val astWithString = typecheck(reify {
      def fun(str: String) = str
    }.tree)
    udfClosure.symbolTable += "str" -> "VARCHAR.VAL"
    val actualStringUDF = UDFCodeGenerator.generateFor(MapUDF(astWithString), udfClosure)
    val expectedStringUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=#VARCHAR.VAL#;"
    actualStringUDF.udf should be(expectedStringUDF)
    actualStringUDF.output.size should be(1)

  }

  "UDFCodeGenerator" should "compile a UDF with a Tuple input containing basic types" in {

    val ast = typecheck(reify {
      def fun(input: (Int, Double, String)) = input._1 + input._2
    }.tree)

    udfClosure.symbolTable += "input._1" -> "TRIPLE.VAL1"
    udfClosure.symbolTable += "input._2" -> "TRIPLE.VAL2"
    val actual = UDFCodeGenerator.generateFor(MapUDF(ast), udfClosure)

    val expectedUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=(#TRIPLE.VAL1#+#TRIPLE.VAL2#);"

    actual.udf should be(expectedUDF)
    actual.output.size should be(1)
  }

  "UDFCodeGenerator" should "compile a UDF with a Tuple input containing case classes and basic types" in {

    val ast = typecheck(reify {
      def fun(input: (Lineitem, Double, String)) = input._1.extendedPrice + input._2
    }.tree)

    udfClosure.symbolTable += "input._1.extendedPrice" -> "LINEITEM.EXTENDEDPRICE"
    udfClosure.symbolTable += "input._2" -> "TRIPLE.VAL2"
    val actual = UDFCodeGenerator.generateFor(MapUDF(ast), udfClosure)

    val expectedUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=(#LINEITEM.EXTENDEDPRICE#+#TRIPLE.VAL2#);"

    actual.udf should be(expectedUDF)
    actual.output.size should be(1)
  }

  ignore should "throw an exception for missing input mappings" in {

    val ast = typecheck(reify {
      def fun(l: Lineitem) = l.lineNumber + l.suppKey
    }.tree)

    the[IllegalArgumentException] thrownBy {
      UDFCodeGenerator.generateFor(MapUDF(ast), udfClosure)
    } should have message "No mapping found for [l.suppKey]"
  }

  ignore should "throw an exception if a UDF parameter type is an arbitrary class" in {

    val ast = typecheck(reify {
      def fun(input: ArbitraryClass) = input.value
    }.tree)

    the[IllegalArgumentException] thrownBy {
      UDFCodeGenerator.generateFor(MapUDF(ast), udfClosure)
    } should have message "ArbitraryClass is not a case class."
  }

  ignore should "throw an exception if a UDF parameter type is a nested case class" in {

    val ast = typecheck(reify {
      def fun(input: Nested) = input.l.partKey
    }.tree)

    the[IllegalArgumentException] thrownBy {
      UDFCodeGenerator.generateFor(MapUDF(ast), udfClosure)
    } should have message "Nested is a nested case class."
  }

  ignore should "throw an exception if a UDF parameter type in a Tuple is an arbitrary class" in {

    val ast = typecheck(reify {
      def fun(input: (Int, ArbitraryClass)) = input._2.value
    }.tree)

    the[IllegalArgumentException] thrownBy {
      UDFCodeGenerator.generateFor(MapUDF(ast), udfClosure)
    } should have message "ArbitraryClass is not a case class."
  }

  "UDFCodeGenerator" should "return correct result type" in {

    val ast = typecheck(reify {
      def fun(l: Lineitem) = l.quantity * l.extendedPrice
    }.tree)

    val actual = UDFCodeGenerator.generateFor(MapUDF(ast), udfClosure)

    val expResIdentifier = nextExpectedOutputIdentifier
    val expectedUDF = s"#<OUT>.$expResIdentifier#=(#LINEITEM.L_QUANTITY#*#LINEITEM.L_EXTENDEDPRICE#);"

    actual.udf should be(expectedUDF)
    actual.output.size should be(1)
    actual.output.head should be(CoGaUDFOutput(TypeName(expResIdentifier), typeOf[Double]))
  }

  "UDFCodeGenerator" should "return multiple results for a Tuple output type" in {

    val ast = typecheck(reify {
      def fun(l: Lineitem) = (l.lineNumber, l.extendedPrice, l.quantity)
    }.tree)

    val actual = UDFCodeGenerator.generateFor(MapUDF(ast), udfClosure)

    val expResIdentifier1 = nextExpectedOutputIdentifier
    val expResIdentifier2 = nextExpectedOutputIdentifier
    val expResIdentifier3 = nextExpectedOutputIdentifier
    val expectedUDF = s"#<OUT>.$expResIdentifier1#=#LINEITEM.L_LINENUMBER#;" +
      s"#<OUT>.$expResIdentifier2#=#LINEITEM.L_EXTENDEDPRICE#;" +
      s"#<OUT>.$expResIdentifier3#=#LINEITEM.L_QUANTITY#;"
    val expectedOutputs = Seq(
      CoGaUDFOutput(TypeName(expResIdentifier1), typeOf[Int]),
      CoGaUDFOutput(TypeName(expResIdentifier2), typeOf[Double]),
      CoGaUDFOutput(TypeName(expResIdentifier3), typeOf[Int])
    )

    actual.udf should be(expectedUDF)
    actual.output.size should be(3)
    for (expected <- expectedOutputs) actual.output should contain(expected)
  }

  "UDFCodeGenerator" should "return multiple results for a Complex output type" in {

    val ast = typecheck(reify {
      def fun(l: Lineitem) = DiscPrice(l.lineNumber, l.extendedPrice * (1 - l.discount))
    }.tree)

    val actual = UDFCodeGenerator.generateFor(MapUDF(ast), udfClosure)

    val expResIdentifier1 = nextExpectedOutputIdentifier
    val expResIdentifier2 = nextExpectedOutputIdentifier
    val expectedUDF = s"#<OUT>.$expResIdentifier1#=#LINEITEM.L_LINENUMBER#;" +
      s"#<OUT>.$expResIdentifier2#=(#LINEITEM.L_EXTENDEDPRICE#*(1-#LINEITEM.L_DISCOUNT#));"
    val expectedOutputs = Seq(
      CoGaUDFOutput(TypeName(expResIdentifier1), typeOf[Int]),
      CoGaUDFOutput(TypeName(expResIdentifier2), typeOf[Double]))

    actual.udf should be(expectedUDF)
    actual.output.size should be(2)
    for (expected <- expectedOutputs) actual.output should contain(expected)
  }

  "UDFCodeGenerator" should "consider point before line calculation" in {

    val ast = typecheck(reify {
      def fun(l: Lineitem) = l.extendedPrice + l.tax * l.discount
    }.tree)

    val actual = UDFCodeGenerator.generateFor(MapUDF(ast), udfClosure)

    val expectedUDF = s"#<OUT>.$nextExpectedOutputIdentifier#=" +
      "(#LINEITEM.L_EXTENDEDPRICE#+(#LINEITEM.L_TAX#*#LINEITEM.L_DISCOUNT#));"

    actual.udf should be(expectedUDF)
    actual.output.size should be(1)
  }

  private def typecheck(ast: Tree): Tree = tb.typecheck(ast).children.head

}

object UDFCodeGeneratorTest {

  var currentExpectedOutputId = 0

  def nextExpectedOutputIdentifier = {
    currentExpectedOutputId = currentExpectedOutputId + 1
    s"RES_$currentExpectedOutputId"
  }

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
