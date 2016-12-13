package org.emmalanguage
package compiler.udf

import org.emmalanguage.compiler.udf.MapUDFTransformerTest._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.language.implicitConversions
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

@RunWith(classOf[JUnitRunner])
class SelectionTransformerTest extends FlatSpec with Matchers with BeforeAndAfter {

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  val symbolTable = Map[String, String](
    "l.lineNumber" -> "LINEITEM.L_LINENUMBER",
    "l.quantity" -> "LINEITEM.L_QUANTITY",
    "l.extendedPrice" -> "LINEITEM.L_EXTENDEDPRICE",
    "l.discount" -> "LINEITEM.L_DISCOUNT",
    "l.tax" -> "LINEITEM.L_TAX"
  )

  "FilterUDFTransformerTest" should "for testing" in {
    val ast = typecheck(reify {
      def fun(l: Lineitem): Double = if (l.quantity > 5) l.extendedPrice else l.discount
    }.tree)

    val actual = new SelectionTransformer(ast, symbolTable).transform

  }
  
  private def typecheck(ast: Tree): Tree = tb.typecheck(ast).children.head

}

object SelectionTransformerTest {

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
