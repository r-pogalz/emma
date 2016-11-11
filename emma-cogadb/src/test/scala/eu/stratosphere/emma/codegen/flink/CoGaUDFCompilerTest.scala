package eu.stratosphere.emma.codegen.cogadb

import eu.stratosphere.emma.codegen.cogadb.CoGaUDFCompiler._
import eu.stratosphere.emma.codegen.cogadb.CoGaUDFCompilerTest.{DiscPrice, Lineitem}
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

@RunWith(classOf[JUnitRunner])
class CoGaUDFCompilerTest extends FlatSpec with Matchers with BeforeAndAfter {

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  var currResId = 0
  val udfClosure = new UDFClosure()

  before {
    currResId = CoGaUDFCompiler.lastOutputId
    udfClosure.symbolTable += "l.lineNumber" -> "LINEITEM.L_LINENUMBER"
    udfClosure.symbolTable += "l.quantity" -> "LINEITEM.L_QUANTITY"
    udfClosure.symbolTable += "l.extendedPrice" -> "LINEITEM.L_EXTENDEDPRICE"
    udfClosure.symbolTable += "l.discount" -> "LINEITEM.L_DISCOUNT"
    udfClosure.symbolTable += "l.tax" -> "LINEITEM.L_TAX"
  }

  //TODO: test missing input values

  "CoGaUDFCompiler" should "throw an exception for missing input mappings" in {
    val ast = typecheck(reify {
      def fun(l: Lineitem) = l.lineNumber + l.suppKey
    }.tree)

    the[IllegalArgumentException] thrownBy {
      CoGaUDFCompiler.compile(ScalaMapUDF(ast), udfClosure)
    } should have message "No mapping found for [l.suppKey]"
  }

  "CoGaUDFCompiler" should "return correct result type" in {
    val ast = typecheck(reify {
      def fun(l: Lineitem) = l.quantity * l.extendedPrice
    }.tree)

    val actual = CoGaUDFCompiler.compile(ScalaMapUDF(ast), udfClosure)

    val expResIdentifier = s"RES_${currResId + 1}"
    val expectedUDF = s"#<OUT>.$expResIdentifier#=(#LINEITEM.L_QUANTITY#*#LINEITEM.L_EXTENDEDPRICE#);"

    actual.udf should be(expectedUDF)
    actual.output.size should be(1)
    actual.output.head should be(CoGaUDFOutput(expResIdentifier, typeOf[Double]))
  }

  "CoGaUDFCompiler" should "return multiple results for a Tuple output type" in {
    val ast = typecheck(reify {
      def fun(l: Lineitem) = (l.lineNumber, l.extendedPrice, l.quantity)
    }.tree)

    val actual = CoGaUDFCompiler.compile(ScalaMapUDF(ast), udfClosure)

    val expResIdentifier1 = s"RES_${currResId + 1}"
    val expResIdentifier2 = s"RES_${currResId + 2}"
    val expResIdentifier3 = s"RES_${currResId + 3}"
    val expectedUDF = s"#<OUT>.$expResIdentifier1#=#LINEITEM.L_LINENUMBER#;" +
      s"#<OUT>.$expResIdentifier2#=#LINEITEM.L_EXTENDEDPRICE#;" +
      s"#<OUT>.$expResIdentifier3#=#LINEITEM.L_QUANTITY#;"
    val expectedOutputs = Seq(CoGaUDFOutput(expResIdentifier1, typeOf[Int]), CoGaUDFOutput(expResIdentifier2,
      typeOf[Double]), CoGaUDFOutput(expResIdentifier3, typeOf[Int]))

    actual.udf should be(expectedUDF)
    actual.output.size should be(3)
    for (expected <- expectedOutputs) actual.output should contain(expected)
  }

  "CoGaUDFCompiler" should "return multiple results for a Complex output type" in {
    val ast = typecheck(reify {
      def fun(l: Lineitem) = DiscPrice(l.lineNumber, l.extendedPrice * (1 - l.discount))
    }.tree)

    val actual = CoGaUDFCompiler.compile(ScalaMapUDF(ast), udfClosure)

    val expResIdentifier1 = s"RES_${currResId + 1}"
    val expResIdentifier2 = s"RES_${currResId + 2}"
    val expectedUDF = s"#<OUT>.$expResIdentifier1#=#LINEITEM.L_LINENUMBER#;" +
      s"#<OUT>.$expResIdentifier2#=(#LINEITEM.L_EXTENDEDPRICE#*(1-#LINEITEM.L_DISCOUNT#));"
    val expectedOutputs = Seq(CoGaUDFOutput(expResIdentifier1, typeOf[Int]), CoGaUDFOutput(expResIdentifier2,
      typeOf[Double]))

    actual.udf should be(expectedUDF)
    actual.output.size should be(2)
    for (expected <- expectedOutputs) actual.output should contain(expected)
  }

  "CoGaUDFCompiler" should "consider point before line calculation" in {
    val ast = typecheck(reify {
      def fun(l: Lineitem) = l.extendedPrice + l.tax * l.discount
    }.tree)

    val actual = CoGaUDFCompiler.compile(ScalaMapUDF(ast), udfClosure)

    val expResIdentifier = s"RES_${currResId + 1}"
    val expectedUDF = s"#<OUT>.$expResIdentifier#=" +
      "(#LINEITEM.L_EXTENDEDPRICE#+(#LINEITEM.L_TAX#*#LINEITEM.L_DISCOUNT#));"

    actual.udf should be(expectedUDF)
    actual.output.size should be(1)
  }

  private def typecheck(ast: Tree): Tree = tb.typecheck(ast).children.head

}

object CoGaUDFCompilerTest {

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

}
