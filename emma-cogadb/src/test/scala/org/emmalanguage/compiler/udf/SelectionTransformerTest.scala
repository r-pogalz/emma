package org.emmalanguage
package compiler.udf

import org.emmalanguage.compiler.udf.MapUDFTransformerTest._
import org.emmalanguage.compiler.udf.common.{UDFClosure, UDFType}
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
    "p" -> "PART")

  //add tests for different predicates
  
  "SelectionTransformerTest" should "for testing" in {

    val ast = typecheck(reify {
      () => (p: Part) => (p.p_size >= p.p_retailprice || p.p_partkey > 0) && p.p_size <= 0.5
    }.tree)

    val actual = new SelectionTransformer(ast, symbolTable).transform

    println(actual)
  }
  
  private def typecheck(ast: Tree): Tree = tb.typecheck(ast).children.head

}

object SelectionTransformerTest {

  case class Part(p_partkey: Int, p_name: String, p_size: Int, p_retailprice: Double, p_comment: String)

}
