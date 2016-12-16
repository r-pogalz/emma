package org.emmalanguage
package compiler.udf

import org.emmalanguage.compiler.udf.MapUDFGeneratorTest._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.language.implicitConversions
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

@RunWith(classOf[JUnitRunner])
class ReduceUDFGeneratorTest extends FlatSpec with Matchers with BeforeAndAfter {

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  val symbolTable = Map[String, String](
    "p" -> "PART",
    "t" -> "TUPLE")

  "ReduceUDFGenerator" should "generate AlgebraicReduceUdf for single stmt" in {

    val z = typecheck(reify {
      0
    }.tree)

    val sngAst = typecheck(reify {
      () => (p: Part) => p.p_size * p.p_partkey
    }.tree)

    val uniAst = typecheck(reify {
      () => (x: Int, y: Int) => x + y
    }.tree)

    val actual = new ReduceUDFGenerator(z, sngAst, uniAst, symbolTable).generate

    println(actual)
  }

  "ReduceUDFGenerator" should "generate AlgebraicReduceUdf for block" in {

    val z = typecheck(reify {
      0
    }.tree)

    val sngAst = typecheck(reify {
      () => (p: Part) => {
        val x = p.p_partkey + 9
        p.p_size * x
      }
    }.tree)

    val uniAst = typecheck(reify {
      () => (x: Int, y: Int) => x + y
    }.tree)

    val actual = new ReduceUDFGenerator(z, sngAst, uniAst, symbolTable).generate

    println(actual)
  }

  "ReduceUDFGenerator" should "generate AlgebraicReduceUdf for two blocks" in {

    val z = typecheck(reify {
      5
    }.tree)

    val sngAst = typecheck(reify {
      () => (p: Part) => {
        val x = p.p_partkey + 9
        p.p_size * x
      }
    }.tree)

    val uniAst = typecheck(reify {
      () => (x: Int, y: Int) => {
        val d = 2 - y
        d + x * 2
      }
    }.tree)

    val actual = new ReduceUDFGenerator(z, sngAst, uniAst, symbolTable).generate

    println(actual)
  }


  private def typecheck(ast: Tree): Tree = tb.typecheck(ast)

}

object ReduceUDFGeneratorTest {

  case class Part(p_partkey: Int, p_name: String, p_size: Int, p_retailprice: Double, p_comment: String)

}
