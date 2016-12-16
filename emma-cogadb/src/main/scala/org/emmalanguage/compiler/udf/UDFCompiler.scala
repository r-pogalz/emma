package org.emmalanguage
package compiler.udf

import org.emmalanguage.compiler.lang.cogadb.ast._
import org.emmalanguage.compiler.udf.UDFCompiler.UDFType.UDFType
import org.emmalanguage.compiler.udf.common._

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

object UDFCompiler {

  object UDFType extends Enumeration {
    type UDFType = Value
    val Map, Filter, Fold = Value
  }

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  def compile(udfType: UDFType, symbolTable: Map[String, String], udfs: String*): Node = udfType match {
    case UDFType.Map => {
      val ast = tb.typecheck(q"${udfs.head}")
      val udfTransformer = new UDFTransformer(MapUDFClosure(ast, symbolTable))
      udfTransformer.transform
    }
    case UDFType.Filter => {
      val ast = tb.typecheck(q"${udfs.head}")
      val udfTransformer = new UDFTransformer(FilterUDFClosure(ast, symbolTable))
      udfTransformer.transform
    }
    case UDFType.Fold => {
      val zElem = tb.typecheck(q"${udfs.head}")
      val sngAst = tb.typecheck(q"${udfs(1)}")
      val uniAst = tb.typecheck(q"${udfs(2)}")
      val udfTransformer = new UDFTransformer(FoldUDFClosure(zElem, sngAst, uniAst, symbolTable))
      udfTransformer.transform
    }
  }
}
