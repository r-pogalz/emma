package org.emmalanguage
package compiler.udf


import scala.collection.mutable
import scala.reflect.runtime.universe._
import internal.reificationSupport._
import org.emmalanguage.compiler.lang.cogadb.ast._
import org.emmalanguage.compiler.udf.common._

class MapUDFGenerator(ast: Tree, symbolTable: Map[String, String]) extends JsonIRGenerator[MapUdf]
  with AnnotatedCCodeGenerator with TypeHelper {

  private val outputs = mutable.ListBuffer.empty[MapUdfOutAttr]

  private val localVars = mutable.Map.empty[TermName, (Type, TermName)]

  def generate: MapUdf = {
    val mapUdfCode = generateAnnotatedCCode(symbolTable, ast, true)
    MapUdf(outputs, mapUdfCode.map(MapUdfCode(_)))
  }

  override def generateOutputExpr(col: TypeName): String = s"#<OUT>.$col#"

  override protected def freshVarName = freshTermName("map_udf_local_var_")

  override protected def newUDFOutput(tpe: Type, infix: String = ""): TypeName = {
    val freshOutputName = freshTypeName(s"MAP_UDF_RES_$infix".toUpperCase)
    outputs += MapUdfOutAttr(tpe.toJsonAttributeType, s"$freshOutputName", s"$freshOutputName")
    freshOutputName
  }

  override protected def basicTypeColumnIdentifier: String = "VALUE"
}
