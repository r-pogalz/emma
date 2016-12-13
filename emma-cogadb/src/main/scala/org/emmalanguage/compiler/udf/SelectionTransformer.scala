package org.emmalanguage.compiler.udf

import scala.collection.mutable
import scala.reflect.runtime.universe._
import internal.reificationSupport._
import org.emmalanguage.compiler.lang.cogadb.ast._
import org.emmalanguage.compiler.udf.common._

class SelectionTransformer(ast: Tree, symbolTable: Map[String, String]) extends UDFTransformer[Selection]
  with TypeHelper {

  def transform: Selection = {
    val mapUdfCode = transformToMapUdfCode(ast, true)
    ???
  }

  private def transformToMapUdfCode(ast: Tree, isPotentialOut: Boolean = false) : Seq[String] = {
    ast match {
      case Function(_, body) =>
        transformToMapUdfCode(body, isPotentialOut)

      case DefDef(_, _, _, _, _, rhs) =>
        transformToMapUdfCode(rhs, isPotentialOut)

      case Block(Nil, expr) =>
        transformToMapUdfCode(expr, isPotentialOut)

      case Apply(typeApply: TypeApply, args: List[Tree]) =>
        ???

      case app@Apply(sel: Select, args: List[Tree]) =>
        ???

      case sel: Select =>
        ???

      case Assign(lhs: Ident, rhs) =>
        ???

      case ValDef(mods, name, tpt: TypeTree, rhs) =>
        ???

      case ifAst: If =>
        ???

      case ide: Ident =>
        ???

      case lit: Literal =>
        ???

      case _ => throw new IllegalArgumentException(s"Code generation for tree type not supported: ${showRaw(ast)}")
    }
  }
}
