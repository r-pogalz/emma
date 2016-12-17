package org.emmalanguage
package compiler.udf

import org.emmalanguage.compiler.lang.cogadb.ast._
import org.emmalanguage.compiler.udf.MapUDFGeneratorTest._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.language.implicitConversions
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

@RunWith(classOf[JUnitRunner])
class SelectionGeneratorTest extends FlatSpec with Matchers with BeforeAndAfter {

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  val symbolTable = Map[String, String](
    "p" -> "PART",
    "n" -> "NESTED_TABLE",
    "i" -> "INT")

  //TODO: add tests for different predicates

  "SelectionGenerator" should "compile column-column predicate" in {

    val ast = typecheck(reify {
      () => (p: Part) => p.p_size == p.p_retailprice
    }.tree)

    val actual = new SelectionGenerator(ast, symbolTable).generate

    val expected = Selection(ColCol(
      AttrRef("PART", "P_SIZE", "P_SIZE", 1),
      AttrRef("PART", "P_RETAILPRICE", "P_RETAILPRICE", 1),
      Equal))
    
    actual should be(expected)
  }

  "SelectionGenerator" should "compile basic types" in {

    val ast = typecheck(reify {
      () => (i: Int) => i <= 10
    }.tree)

    val actual = new SelectionGenerator(ast, symbolTable).generate

    println(actual)
  }

  "SelectionGenerator" should "compile column-constant predicate" in {

    val ast = typecheck(reify {
      () => (p: Part) => p.p_size < 5
    }.tree)

    val actual = new SelectionGenerator(ast, symbolTable).generate

    println(actual)
  }

  "SelectionGenerator" should "compile constant-column predicate" in {

    val ast = typecheck(reify {
      () => (p: Part) => 5 > p.p_size
    }.tree)

    val actual = new SelectionGenerator(ast, symbolTable).generate

    println(actual)
  }

  "SelectionGenerator" should "compile disjunction" in {

    val ast = typecheck(reify {
      () => (p: Part) => p.p_size != p.p_retailprice || p.p_partkey <= 0
    }.tree)

    val actual = new SelectionGenerator(ast, symbolTable).generate

    println(actual)
  }

  "SelectionGenerator" should "compile conjunction" in {

    val ast = typecheck(reify {
      () => (p: Part) => p.p_size >= p.p_retailprice && p.p_partkey <= p.p_retailprice
    }.tree)

    val actual = new SelectionGenerator(ast, symbolTable).generate

    println(actual)
  }

  "SelectionGenerator" should "compile a composite filter predicate" in {

    val ast = typecheck(reify {
      () => (p: Part) => (p.p_size >= p.p_retailprice || p.p_partkey > 0) && p.p_size <= 0.5
    }.tree)

    val actual = new SelectionGenerator(ast, symbolTable).generate

    println(actual)
  }

  case class Nested(p: Part)

  "SelectionGenerator" should "compile a nested input type" in {

    val ast = typecheck(reify {
      () => (n: Nested) => n.p.p_size >= n.p.p_retailprice || (n.p.p_partkey > 0 && n.p.p_size <= 0.5)
    }.tree)

    val actual = new SelectionGenerator(ast, symbolTable).generate

    println(actual)
  }

  private def typecheck(ast: Tree): Tree = tb.typecheck(ast).children.head

}

object SelectionGeneratorTest {

  case class Part(p_partkey: Int, p_name: String, p_size: Int, p_retailprice: Double, p_comment: String)

}
