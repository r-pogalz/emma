package org.emmalanguage
package compiler.udf

import eu.stratosphere.emma.api.DataBag
import org.emmalanguage.compiler.udf.MapUDFTransformerTest._
import org.emmalanguage.compiler.udf.common.UDFClosure
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.emmalanguage.compiler.lang.cogadb.ast._

import scala.language.implicitConversions
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox
import org.emmalanguage.compiler.udf.common.UDFType

@RunWith(classOf[JUnitRunner])
class UDFCodeGeneratorTest extends FlatSpec with Matchers with BeforeAndAfter {

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  implicit def MapUdfWrapper(t: MapUdf) = new MapUdfHelperClass(t)

  final class MapUdfHelperClass(udf: MapUdf) {
    implicit def concatenated: String = udf.mapUdfCode.map(_.code).mkString
  }

  val symbolTable = Map[String, String](
    "p" -> "PART",
     "t" -> "TUPLE")

  "UDFCodeGenerator" should "rewrite filter single statement to a MapUdf" in {

    val ast = typecheck(reify {
      () => (p: Part) => p.p_size * 2 >= p.p_retailprice
    }.tree)

    val actual = new UDFCodeGenerator(UDFClosure(ast, symbolTable, UDFType.Filter)).generate

    val expectedUDF = "bool filter_udf_condition=((#PART.P_SIZE#*2)>=#PART.P_RETAILPRICE#);" +
      "if(filter_udf_condition){" +
      "#<OUT>.MAP_UDF_RES_P_PARTKEY_1#=#PART.P_PARTKEY#;" +
      "#<OUT>.MAP_UDF_RES_P_NAME_1#=#PART.P_NAME#;" +
      "#<OUT>.MAP_UDF_RES_P_SIZE_1#=#PART.P_SIZE#;" +
      "#<OUT>.MAP_UDF_RES_P_RETAILPRICE_1#=#PART.P_RETAILPRICE#;" +
      "#<OUT>.MAP_UDF_RES_P_COMMENT_1#=#PART.P_COMMENT#;" +
      "}else{" +
      "NONE;" +
      "}"

    actual.asInstanceOf[MapUdf].concatenated should be(expectedUDF)
  }

  "UDFCodeGenerator" should "rewrite filter function block with tuple input to a MapUdf" in {

    val ast = typecheck(reify {
      () => (t: (Int, Double, String)) => {
        val x = t._1 * 2
        x >= t._2
      }
    }.tree)

    val actual = new UDFCodeGenerator(UDFClosure(ast, symbolTable, UDFType.Filter)).generate
    
    val expectedUDF = "int32_t x=(#TUPLE._1#*2);" +
      "bool filter_udf_condition=(x>=#TUPLE._2#);" +
      "if(filter_udf_condition){" +
      "#<OUT>.MAP_UDF_RES__1_1#=#TUPLE._1#;" +
      "#<OUT>.MAP_UDF_RES__2_1#=#TUPLE._2#;" +
      "#<OUT>.MAP_UDF_RES__3_1#=#TUPLE._3#;" +
      "}else{" +
      "NONE;}"

    actual.asInstanceOf[MapUdf].concatenated should be(expectedUDF)
  }

  "UDFCodeGenerator" should "TEST" in {

    val ast = typecheck(reify {
      () => (p: Part) => (p.p_size >= p.p_retailprice || p.p_partkey > 0) && p.p_size <= 0.5
    }.tree)

    val actual = new UDFCodeGenerator(UDFClosure(ast, symbolTable, UDFType.Filter)).generate

    println(actual)
  }

  "UDFCodeGenerator" should "generate Selection JSON with column-column predicate" in {

    val ast = typecheck(reify {
      () => (p: Part) => p.p_size >= p.p_retailprice
    }.tree)

    val actual = new UDFCodeGenerator(UDFClosure(ast, symbolTable, UDFType.Filter)).generate

    println(actual)
  }

  "UDFCodeGenerator" should "throw an exception if UDF is DefDef" in {

    val ast = typecheck(reify {
      def fun(p: Part) = p.p_partkey + p.p_size
    }.tree)

    val thrown = intercept[IllegalArgumentException] {
      new UDFCodeGenerator(UDFClosure(ast, symbolTable, UDFType.Map)).generate
    }
    thrown.getMessage should startWith regex ("Scala AST is not a Function")
  }
  
  private def typecheck(ast: Tree): Tree = tb.typecheck(ast)

}

object UDFCodeGeneratorTest {

  case class Part(p_partkey: Int, p_name: String, p_size: Int, p_retailprice: Double, p_comment: String)

}
