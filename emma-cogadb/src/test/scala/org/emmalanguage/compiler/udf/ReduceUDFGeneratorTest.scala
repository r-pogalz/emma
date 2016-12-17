package org.emmalanguage
package compiler.udf

import org.emmalanguage.compiler.lang.cogadb.ast._
import org.emmalanguage.compiler.udf.ReduceUDFGeneratorTest._
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

    val expectedReduceOutName = nextExpectedOutputIdentifier
    val expected = AlgebraicReduceUdf(
      List(ReduceUdfAttr("INT", "intermediate_reduce_res", IntConst(0))),
      List(ReduceUdfOutAttr("INT", expectedReduceOutName, expectedReduceOutName)),
      List(ReduceUdfCode("int32_t local_map_res=(#PART.P_SIZE#*#PART.P_PARTKEY#);"),
        ReduceUdfCode("#<hash_entry>.intermediate_reduce_res#=" +
          "(#<hash_entry>.intermediate_reduce_res#+local_map_res);")),
      List(ReduceUdfCode(s"#<OUT>.$expectedReduceOutName#=#<hash_entry>.intermediate_reduce_res#;")))

    actual should be(expected)
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

    val expectedReduceOutName = nextExpectedOutputIdentifier
    val expected = AlgebraicReduceUdf(
      List(ReduceUdfAttr("INT", "intermediate_reduce_res", IntConst(0))),
      List(ReduceUdfOutAttr("INT", expectedReduceOutName, expectedReduceOutName)),
      List(ReduceUdfCode("int32_t x=(#PART.P_PARTKEY#+9);"),
        ReduceUdfCode("int32_t local_map_res=(#PART.P_SIZE#*x);"),
        ReduceUdfCode("#<hash_entry>.intermediate_reduce_res#=" +
          "(#<hash_entry>.intermediate_reduce_res#+local_map_res);")),
      List(ReduceUdfCode(s"#<OUT>.$expectedReduceOutName#=#<hash_entry>.intermediate_reduce_res#;")))
    
    actual should be(expected)
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

    val expectedReduceOutName = nextExpectedOutputIdentifier
    val expected = AlgebraicReduceUdf(
      List(ReduceUdfAttr("INT", "intermediate_reduce_res", IntConst(5))),
      List(ReduceUdfOutAttr("INT", expectedReduceOutName, expectedReduceOutName)),
      List(ReduceUdfCode("int32_t x=(#PART.P_PARTKEY#+9);"),
        ReduceUdfCode("int32_t local_map_res=(#PART.P_SIZE#*x);"),
        ReduceUdfCode("int32_t d=(2-local_map_res);"),
        ReduceUdfCode("#<hash_entry>.intermediate_reduce_res#=(d+(#<hash_entry>.intermediate_reduce_res#*2));")),
      List(ReduceUdfCode(s"#<OUT>.$expectedReduceOutName#=#<hash_entry>.intermediate_reduce_res#;")))

    actual should be(expected)
  }


  private def typecheck(ast: Tree): Tree = tb.typecheck(ast)

}

object ReduceUDFGeneratorTest {

  var currentExpectedOutputId = 0

  def nextExpectedOutputIdentifier = {
    currentExpectedOutputId = currentExpectedOutputId + 1
    s"REDUCE_UDF_RES_$currentExpectedOutputId"
  }

  case class Part(p_partkey: Int, p_name: String, p_size: Int, p_retailprice: Double, p_comment: String)

}
