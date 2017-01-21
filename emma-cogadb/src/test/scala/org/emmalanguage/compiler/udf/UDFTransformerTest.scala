package org.emmalanguage
package compiler.udf

import org.emmalanguage.compiler.lang.cogadb.ast._
import org.emmalanguage.compiler.udf.UDFTransformerTest._
import org.emmalanguage.compiler.udf.common._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.math.sqrt
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

@RunWith(classOf[JUnitRunner])
class UDFTransformerTest extends FlatSpec with Matchers with BeforeAndAfter {

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  //================= TEST CASES FOR FUNCTIONAL EVALUATION =======================
  
  "UDFTransformer" should "compile map UDF with primitive input (01)" in {

    val ast = typecheck(reify {
      () => (d: Double) => d
    }.tree)

    val actual = new UDFTransformer(MapUDFClosure(ast, Map[String, String]("d" -> "DOUBLE"))).transform

    val expectedOutIde = nextExpectedOutputIdentifier("MAP_UDF_RES_")
    val expected = MapUdf(Seq(MapUdfOutAttr("DOUBLE", expectedOutIde, expectedOutIde)),
      Seq(MapUdfCode(s"#<OUT>.$expectedOutIde#=#DOUBLE.VALUE#;")))

    actual should be(expected)
  }

  "UDFTransformer" should "compile map UDF with complex input (02)" in {

    val ast = typecheck(reify {
      () => (p: Part) => p.p_partkey
    }.tree)

    val actual = new UDFTransformer(MapUDFClosure(ast, Map[String, String]("p" -> "PART"))).transform

    val expectedOutIde = nextExpectedOutputIdentifier("MAP_UDF_RES_")
    val expected = MapUdf(Seq(MapUdfOutAttr("INT", expectedOutIde, expectedOutIde)),
      Seq(MapUdfCode(s"#<OUT>.$expectedOutIde#=#PART.P_PARTKEY#;")))

    actual should be(expected)
  }

  "UDFTransformer" should "compile map UDF with tuple input (03)" in {

    val ast = typecheck(reify {
      () => (t: (Int, Float, Double)) => t._2
    }.tree)

    val actual = new UDFTransformer(MapUDFClosure(ast, Map[String, String]("t" -> "TUPLE3"))).transform

    val expectedOutIde = nextExpectedOutputIdentifier("MAP_UDF_RES_")
    val expected = MapUdf(Seq(MapUdfOutAttr("FLOAT", expectedOutIde, expectedOutIde)),
      Seq(MapUdfCode(s"#<OUT>.$expectedOutIde#=#TUPLE3._2#;")))

    actual should be(expected)
  }

  "UDFTransformer" should "compile map UDF with nested input (04)" in {

    val ast = typecheck(reify {
      () => (n: (Part, Int)) => n._1.p_retailprice
    }.tree)

    val actual = new UDFTransformer(MapUDFClosure(ast, Map[String, String]("n" -> "UNNESTED"))).transform

    val expectedOutIde = nextExpectedOutputIdentifier("MAP_UDF_RES_")
    val expected = MapUdf(Seq(MapUdfOutAttr("DOUBLE", expectedOutIde, expectedOutIde)),
      Seq(MapUdfCode(s"#<OUT>.$expectedOutIde#=#UNNESTED._1_P_RETAILPRICE#;")))

    actual should be(expected)
  }

  "UDFTransformer" should "compile map UDF with complex output (05)" in {

    val ast = typecheck(reify {
      () => (p: Part) => IntermediateRes(p.p_partkey, p.p_retailprice)
    }.tree)

    val actual = new UDFTransformer(MapUDFClosure(ast, Map[String, String]("p" -> "PART"))).transform

    val expOutIde1 = nextExpectedOutputIdentifier("MAP_UDF_RES_PARTKEY_")
    val expOutIde2 = nextExpectedOutputIdentifier("MAP_UDF_RES_RETAILPRICE_")
    val expected = MapUdf(Seq(MapUdfOutAttr("INT", expOutIde1, expOutIde1),
      MapUdfOutAttr("DOUBLE", expOutIde2, expOutIde2)),
      Seq(MapUdfCode(s"#<OUT>.$expOutIde1#=#PART.P_PARTKEY#;"),
        MapUdfCode(s"#<OUT>.$expOutIde2#=#PART.P_RETAILPRICE#;")))

    actual should be(expected)
  }

  "UDFTransformer" should "compile map UDF with tuple output (06)" in {

    val ast = typecheck(reify {
      () => (p: Part) => (p.p_size, IntermediateRes(p.p_partkey, p.p_retailprice))
    }.tree)

    val actual = new UDFTransformer(MapUDFClosure(ast, Map[String, String]("p" -> "PART"))).transform

    val expOutIde1 = nextExpectedOutputIdentifier("MAP_UDF_RES__1_")
    val expOutIde2 = nextExpectedOutputIdentifier("MAP_UDF_RES__2_PARTKEY_")
    val expOutIde3 = nextExpectedOutputIdentifier("MAP_UDF_RES__2_RETAILPRICE_")
    val expected = MapUdf(Seq(MapUdfOutAttr("INT", expOutIde1, expOutIde1),
      MapUdfOutAttr("INT", expOutIde2, expOutIde2),
      MapUdfOutAttr("DOUBLE", expOutIde3, expOutIde3)),
      Seq(MapUdfCode(s"#<OUT>.$expOutIde1#=#PART.P_SIZE#;"),
        MapUdfCode(s"#<OUT>.$expOutIde2#=#PART.P_PARTKEY#;"),
        MapUdfCode(s"#<OUT>.$expOutIde3#=#PART.P_RETAILPRICE#;")))

    actual should be(expected)
  }

  "UDFTransformer" should "compile map UDF with nested input projection (07)" in {

    val ast = typecheck(reify {
      () => (in: (Int, IntermediateRes)) => in
    }.tree)

    val actual = new UDFTransformer(MapUDFClosure(ast, Map[String, String]("in" -> "UNNESTED2"))).transform

    val expOutIde1 = nextExpectedOutputIdentifier("MAP_UDF_RES__1_")
    val expOutIde2 = nextExpectedOutputIdentifier("MAP_UDF_RES__2_PARTKEY_")
    val expOutIde3 = nextExpectedOutputIdentifier("MAP_UDF_RES__2_RETAILPRICE_")
    val expected = MapUdf(Seq(MapUdfOutAttr("INT", expOutIde1, expOutIde1),
      MapUdfOutAttr("INT", expOutIde2, expOutIde2),
      MapUdfOutAttr("DOUBLE", expOutIde3, expOutIde3)),
      Seq(MapUdfCode(s"#<OUT>.$expOutIde1#=#UNNESTED2._1#;"),
        MapUdfCode(s"#<OUT>.$expOutIde2#=#UNNESTED2._2_PARTKEY#;"),
        MapUdfCode(s"#<OUT>.$expOutIde3#=#UNNESTED2._2_RETAILPRICE#;")))

    actual should be(expected)
  }

  "UDFTransformer" should "compile map UDF with arithmetic operations (08)" in {

    val ast = typecheck(reify {
      () => (p: Part) => {
        val localVar1 = (p.p_partkey + p.p_size) * (p.p_size - 1)
        var localVar2 = localVar1 / 2.0
        localVar2
      }
    }.tree)

    val actual = new UDFTransformer(MapUDFClosure(ast, Map[String, String]("p" -> "PART"))).transform

    val expectedOutIde = nextExpectedOutputIdentifier("MAP_UDF_RES_")
    val expected = MapUdf(Seq(MapUdfOutAttr("DOUBLE", expectedOutIde, expectedOutIde)),
      Seq(MapUdfCode("int32_t localVar1=((#PART.P_PARTKEY#+#PART.P_SIZE#)*(#PART.P_SIZE#-1));"),
        MapUdfCode("double localVar2=(localVar1/2.0);"),
        MapUdfCode(s"#<OUT>.$expectedOutIde#=localVar2;")))

    actual should be(expected)
  }

  "UDFTransformer" should "compile map UDF with math library function calls (09)" in {

    val ast = typecheck(reify {
      () => (cross$1: (Point, Point)) =>
        sqrt(scala.math.pow(cross$1._1.x - cross$1._2.x, 2) + scala.math.pow(cross$1._1.y - cross$1._2.y, 2))
    }.tree)

    val actual = new UDFTransformer(MapUDFClosure(ast, Map[String, String]("cross$1" -> "CROSS$1"))).transform

    val expectedOutIde = nextExpectedOutputIdentifier("MAP_UDF_RES_")
    val expected = MapUdf(Seq(MapUdfOutAttr("DOUBLE", expectedOutIde, expectedOutIde)),
      Seq(MapUdfCode(s"#<OUT>.$expectedOutIde#=" +
        "sqrt((pow((#CROSS$1._1_X#-#CROSS$1._2_X#),2.0)+pow((#CROSS$1._1_Y#-#CROSS$1._2_Y#),2.0)));")))

    actual should be(expected)
  }

  "UDFTransformer" should "compile fold combinator with min UDF (10)" in {

    val z = typecheck(reify {
      1000
    }.tree)

    val sngAst = typecheck(reify {
      () => (p: Part) => p.p_size
    }.tree)

    val uniAst = typecheck(reify {
      () => (x: Int, y: Int) => {
        if (x > y) y else x
      }
    }.tree)

    val actual = new UDFTransformer(FoldUDFClosure(z, sngAst, uniAst, Map[String, String]("p" -> "PART")))
                 .transform


    val expectedOutIde = nextExpectedOutputIdentifier("REDUCE_UDF_RES_")
    val expectedLocalVar = nextExpectedOutputIdentifier("reduce_udf_local_var_")
    val expected = AlgebraicReduceUdf(
      List(ReduceUdfAttr("INT", "intermediate_reduce_res", IntConst(1000))),
      List(ReduceUdfOutAttr("INT", expectedOutIde, expectedOutIde)),
      List(ReduceUdfCode("int32_t local_map_res=#PART.P_SIZE#;"),
        ReduceUdfCode(s"int32_t $expectedLocalVar;"),
        ReduceUdfCode("if((#<hash_entry>.intermediate_reduce_res#>local_map_res)){"),
        ReduceUdfCode(s"$expectedLocalVar=local_map_res;"),
        ReduceUdfCode("}else{"),
        ReduceUdfCode(s"$expectedLocalVar=#<hash_entry>.intermediate_reduce_res#;"),
        ReduceUdfCode("}"),
        ReduceUdfCode(s"#<hash_entry>.intermediate_reduce_res#=$expectedLocalVar;")),
      List(ReduceUdfCode(s"#<OUT>.$expectedOutIde#=#<hash_entry>.intermediate_reduce_res#;")))

    actual should be(expected)
  }

  "UDFTransformer" should "compile fold combinator with arithmetic operations in UDFs (11)" in {

    val z = typecheck(reify {
      0.0
    }.tree)

    val sngAst = typecheck(reify {
      () => (p: Part) => {
        val x = p.p_partkey + p.p_size
        p.p_retailprice * x
      }
    }.tree)

    val uniAst = typecheck(reify {
      () => (x: Double, y: Double) => {
        val d = y - 1
        d + x * 2
      }
    }.tree)

    val actual = new UDFTransformer(FoldUDFClosure(z, sngAst, uniAst, Map[String, String]("p" -> "PART")))
                 .transform

    val expectedOutIde = nextExpectedOutputIdentifier("REDUCE_UDF_RES_")
    val expected = AlgebraicReduceUdf(
      List(ReduceUdfAttr("DOUBLE", "intermediate_reduce_res", DoubleConst(0.0))),
      List(ReduceUdfOutAttr("DOUBLE", expectedOutIde, expectedOutIde)),
      List(ReduceUdfCode("int32_t x=(#PART.P_PARTKEY#+#PART.P_SIZE#);"),
        ReduceUdfCode("double local_map_res=(#PART.P_RETAILPRICE#*x);"),
        ReduceUdfCode("double d=(local_map_res-1);"),
        ReduceUdfCode("#<hash_entry>.intermediate_reduce_res#=(d+(#<hash_entry>.intermediate_reduce_res#*2));")),
      List(ReduceUdfCode(s"#<OUT>.$expectedOutIde#=#<hash_entry>.intermediate_reduce_res#;")))

    actual should be(expected)
  }

  ignore should "compile fold combinator with complex output (12)" in {

    val z = typecheck(reify {
      IntermediateRes(0, 0.0)
    }.tree)

    val sngAst = typecheck(reify {
      () => (p: Part) => IntermediateRes(p.p_partkey, p.p_retailprice)
    }.tree)

    val uniAst = typecheck(reify {
      () => (x: IntermediateRes, y: IntermediateRes) =>
        IntermediateRes(x.partkey + y.partkey, x.retailprice + y.retailprice)
    }.tree)

    val actual = new UDFTransformer(FoldUDFClosure(z, sngAst, uniAst, Map[String, String]("p" -> "PART")))
                 .transform

    val expected = AlgebraicReduceUdf(
      List(ReduceUdfAttr("INT", "intermediate_reduce_res_1", IntConst(0)),
        ReduceUdfAttr("DOUBLE", "intermediate_reduce_res_2", DoubleConst(0.0))),
      List(ReduceUdfOutAttr("INT", "REDUCE_UDF_RES_1", "REDUCE_UDF_RES_1"),
        ReduceUdfOutAttr("DOUBLE", "REDUCE_UDF_RES_2", "REDUCE_UDF_RES_2")),
      List(ReduceUdfCode("int32_t local_map_res_1=#PART.P_PARTKEY#;"),
        ReduceUdfCode("int32_t local_map_res_2=#PART.P_RETAILPRICE#;"),
        ReduceUdfCode("#<hash_entry>.intermediate_reduce_res_1#=" +
          "(#<hash_entry>.intermediate_reduce_res_1#+local_map_res_1);"),
        ReduceUdfCode("#<hash_entry>.intermediate_reduce_res_2#=" +
          "(#<hash_entry>.intermediate_reduce_res_2#+local_map_res_2);")),
      List(ReduceUdfCode("#<OUT>.REDUCE_UDF_RES_1#=#<hash_entry>.intermediate_reduce_res_1#;"),
        ReduceUdfCode("#<OUT>.REDUCE_UDF_RES_2#=#<hash_entry>.intermediate_reduce_res_2#;")))

    actual should be(expected)
  }

  "UDFTransformer" should "rewrite single stmt withFilter UDF with arithmetic operations to a MapUdf (13)" in {

    val ast = typecheck(reify {
      () => (d: Double) => d * 2 >= 10
    }.tree)

    val actual = new UDFTransformer(FilterUDFClosure(ast, Map[String, String]("d" -> "DOUBLE"))).transform

    val expectedOutIde = nextExpectedOutputIdentifier("MAP_UDF_RES_")
    val expected = MapUdf(Seq(MapUdfOutAttr("DOUBLE", expectedOutIde, expectedOutIde)),
      Seq(MapUdfCode("bool filter_udf_condition=((#DOUBLE.VALUE#*2)>=10);"),
        MapUdfCode("if(filter_udf_condition){"),
        MapUdfCode(s"#<OUT>.$expectedOutIde#=#DOUBLE.VALUE#;"),
        MapUdfCode("}else{"),
        MapUdfCode("}")))

    actual should be(expected)
  }

  "UDFTransformer" should "rewrite withFilter UDF with multiple statements to a MapUdf (14)" in {

    val ast = typecheck(reify {
      () => (t: (Int, Float, Double)) => {
        val x = t._1 * 2
        x >= t._2
      }
    }.tree)

    val actual = new UDFTransformer(FilterUDFClosure(ast, Map[String, String]("t" -> "TUPLE3"))).transform

    val expectedOutIde1 = nextExpectedOutputIdentifier("MAP_UDF_RES__1_")
    val expectedOutIde2 = nextExpectedOutputIdentifier("MAP_UDF_RES__2_")
    val expectedOutIde3 = nextExpectedOutputIdentifier("MAP_UDF_RES__3_")
    val expected = MapUdf(Seq(MapUdfOutAttr("INT", expectedOutIde1, expectedOutIde1),
      MapUdfOutAttr("FLOAT", expectedOutIde2, expectedOutIde2),
      MapUdfOutAttr("DOUBLE", expectedOutIde3, expectedOutIde3)),
      Seq(MapUdfCode("int32_t x=(#TUPLE3._1#*2);"),
        MapUdfCode("bool filter_udf_condition=(x>=#TUPLE3._2#);"),
        MapUdfCode("if(filter_udf_condition){"),
        MapUdfCode(s"#<OUT>.$expectedOutIde1#=#TUPLE3._1#;"),
        MapUdfCode(s"#<OUT>.$expectedOutIde2#=#TUPLE3._2#;"),
        MapUdfCode(s"#<OUT>.$expectedOutIde3#=#TUPLE3._3#;"),
        MapUdfCode("}else{"),
        MapUdfCode("}")))

    actual should be(expected)
  }

  "UDFTransformer" should "compile withFilter UDF with simple predicates to GenericSelection (15)" in {

    val ast = typecheck(reify {
      () => (p: Part) => p.p_size > p.p_retailprice || p.p_partkey <= 25
    }.tree)

    val actual = new UDFTransformer(FilterUDFClosure(ast, Map[String, String]("p" -> "PART"))).transform

    val expectedOutIde1 = nextExpectedOutputIdentifier("MAP_UDF_RES_")
    val expectedOutIde2 = nextExpectedOutputIdentifier("MAP_UDF_RES_")
    val expectedOutIde3 = nextExpectedOutputIdentifier("MAP_UDF_RES_")
    val expected = Selection(Or(Seq(ColCol(AttrRef("PART", "P_SIZE", "P_SIZE"),
      AttrRef("PART", "P_RETAILPRICE", "P_RETAILPRICE"), GreaterThan),
      ColConst(AttrRef("PART", "P_PARTKEY", "P_PARTKEY"), IntConst(25), LessEqual))))

    actual should be(expected)
  }

  "UDFTransformer" should "compile withFilter UDF with nested predicates to GenericSelection (16)" in {

    val ast = typecheck(reify {
      () => (p: Part) => (p.p_size > p.p_retailprice || p.p_partkey <= 25) && p.p_size >= 5
    }.tree)

    val actual = new UDFTransformer(FilterUDFClosure(ast, Map[String, String]("p" -> "PART"))).transform

    val expectedOutIde1 = nextExpectedOutputIdentifier("MAP_UDF_RES_")
    val expectedOutIde2 = nextExpectedOutputIdentifier("MAP_UDF_RES_")
    val expectedOutIde3 = nextExpectedOutputIdentifier("MAP_UDF_RES_")
    val expected = Selection(
      And(Seq(
        Or(Seq(ColCol(AttrRef("PART", "P_SIZE", "P_SIZE"),
          AttrRef("PART", "P_RETAILPRICE", "P_RETAILPRICE"), GreaterThan),
          ColConst(AttrRef("PART", "P_PARTKEY", "P_PARTKEY"), IntConst(25), LessEqual))),
        ColConst(AttrRef("PART", "P_SIZE", "P_SIZE"), IntConst(5), GreaterEqual))))

    actual should be(expected)
  }

  private def typecheck(ast: Tree): Tree = tb.typecheck(ast)

}

object UDFTransformerTest {
  var currentExpectedOutputId = 0

  var outputIdentifiers: Map[String, Int] = Map[String, Int]()

  def nextExpectedOutputIdentifier(prefix: String) = {
    outputIdentifiers.get(prefix) match {
      case Some(x) => {
        currentExpectedOutputId = x + 1
        outputIdentifiers += (prefix -> currentExpectedOutputId)
        s"$prefix$currentExpectedOutputId"
      }
      case None => {
        currentExpectedOutputId = 1
        outputIdentifiers += (prefix -> currentExpectedOutputId)
        s"$prefix$currentExpectedOutputId"
      }
    }
  }

  case class Part(p_partkey: Int, p_name: String, p_size: Int, p_retailprice: Double, p_comment: String)

  case class IntermediateRes(partkey: Int, retailprice: Double)

  case class Point(x: Double, y: Double)

}
