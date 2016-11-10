package eu.stratosphere.emma.codegen.cogadb

import eu.stratosphere.emma.codegen.cogadb.CoGaUDFCompiler.UDFClosure
import eu.stratosphere.emma.runtime.CoGaDB
import eu.stratosphere.emma.testutil._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

@RunWith(classOf[JUnitRunner])
class CoGaUDFCompilerTest extends FlatSpec with Matchers with BeforeAndAfter {

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  private def engine =
    CoGaDB.testing()
  
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

  "CoGaUDFCompiler" should "return correct result type" in withRuntime(engine) {
    case flink: CoGaDB =>
      val tree = reify{ def times(l: Lineitem) = l.quantity * l.extendedPrice }.tree
      val toCompile = tb.typecheck(tree).children.head

      val actual = CoGaUDFCompiler.compile(toCompile, udfClosure)

      val expResIdentifier = s"RES_${currResId + 1}"
      val expectedUDF = s"#<OUT>.$expResIdentifier#=(#LINEITEM.L_QUANTITY#*#LINEITEM.L_EXTENDEDPRICE#);"
      assertResult(expectedUDF)(actual.udf)
      assertResult(1)(actual.output.size)
      assertResult(expResIdentifier)(actual.output.head.identifier)
      assertResult("Double")(actual.output.head.tpe.toString)
  }

  "CoGaUDFCompiler" should "return multiple results for a Tuple output type" in withRuntime(engine) {
    case flink: CoGaDB =>
      val tree = reify{ def times(l: Lineitem) = (l.lineNumber, l.extendedPrice, l.quantity) }.tree
      val toCompile = tb.typecheck(tree).children.head

      val actual = CoGaUDFCompiler.compile(toCompile, udfClosure)

      val expResIdentifier1 = s"RES_${currResId + 1}"
      val expResIdentifier2 = s"RES_${currResId + 2}"
      val expResIdentifier3 = s"RES_${currResId + 3}"
      val expectedUDF = s"#<OUT>.$expResIdentifier1#=#LINEITEM.L_LINENUMBER#;" +
                        s"#<OUT>.$expResIdentifier2#=#LINEITEM.L_EXTENDEDPRICE#;" +
                        s"#<OUT>.$expResIdentifier3#=#LINEITEM.L_QUANTITY#;"
      assertResult(expectedUDF)(actual.udf)

      val expectedOutputs = Seq((expResIdentifier1, "Int"), (expResIdentifier2, "Double"), (expResIdentifier3, "Int"))
      assertResult(3)(actual.output.size)
      for(e <- expectedOutputs) assert(actual.output.exists(o => o.identifier == e._1 && o.tpe.toString == e._2))
  }

  "CoGaUDFCompiler" should "return multiple results for a Complex output type" in withRuntime(engine) {
    case flink: CoGaDB =>
      val tree = reify{ def times(l: Lineitem) = DiscPrice(l.lineNumber, l.extendedPrice * (1 - l.discount)) }.tree
      val toCompile = tb.typecheck(tree).children.head

      val actual = CoGaUDFCompiler.compile(toCompile, udfClosure)

      val expResIdentifier1 = s"RES_${currResId + 1}"
      val expResIdentifier2 = s"RES_${currResId + 2}"
      val expectedUDF = s"#<OUT>.$expResIdentifier1#=#LINEITEM.L_LINENUMBER#;" +
                        s"#<OUT>.$expResIdentifier2#=(#LINEITEM.L_EXTENDEDPRICE#*(1-#LINEITEM.L_DISCOUNT#));"
      assertResult(expectedUDF)(actual.udf)

      val expectedOutputs = Seq((expResIdentifier1, "Int"), (expResIdentifier2, "Double"))
      assertResult(2)(actual.output.size)
      for(e <- expectedOutputs) assert(actual.output.exists(o => o.identifier == e._1 && o.tpe.toString == e._2))
  }
  
  "CoGaUDFCompiler" should "consider point before line calculation" in withRuntime(engine) {
    case flink: CoGaDB =>
      val tree = reify{ def fun(l: Lineitem) = l.extendedPrice + l.tax * l.discount }.tree
      val toCompile = tb.typecheck(tree).children.head

      val actual = CoGaUDFCompiler.compile(toCompile, udfClosure)

      val expResIdentifier = s"RES_${currResId + 1}"
      val expectedUDF = s"#<OUT>.$expResIdentifier#=" +
                         "(#LINEITEM.L_EXTENDEDPRICE#+(#LINEITEM.L_TAX#*#LINEITEM.L_DISCOUNT#));"
      assertResult(expectedUDF)(actual.udf)
      assertResult(1)(actual.output.size)
  }
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
