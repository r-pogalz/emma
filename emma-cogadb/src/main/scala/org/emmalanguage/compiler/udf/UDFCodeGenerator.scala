package org.emmalanguage
package compiler.udf

import org.emmalanguage.compiler.udf.common._
import compiler.lang.cogadb.ast._

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox


class UDFCodeGenerator(udfClosure: UDFClosure) {

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  def generate: Node = udfClosure.ast match {
    case Function => udfClosure.udfType match {
      case UDFType.Map => new MapUDFTransformer(udfClosure.ast, udfClosure.symTbl).transform
      case UDFType.Filter => generateForFilterUDF
      case UDFType.Fold => throw new IllegalArgumentException("Not implemented yet")
    }
    case _ => throw new IllegalArgumentException(s"Scala AST is not a Function: ${showRaw(udfClosure.ast)}")
  }

  private def generateForFilterUDF: Node = {
    val newAst = filterUDFTransformer.transform(udfClosure.ast)
    val untypechecked = tb.untypecheck(newAst)
    val typechecked = tb.typecheck(untypechecked)

    ???
  }

  private object filterUDFTransformer extends Transformer {
    override def transform(tree: Tree) = tree match {
      case fun@Function(vparams, body) =>
        treeCopy.Function(fun, vparams, rewrite(tree, body, vparams))
      case t => super.transform(t)
    }

    def rewrite(parent: Tree, t: Tree, params: List[ValDef]): Tree = t match {
      case b: Block => transformBlock(b, params.head)
      case _ => super.transform(parent)
    }

    def transformBlock(b: Block, param: ValDef): Block = {
      val varName = TermName("filter_udf_condition")
      val filterCond = q" val $varName: Boolean = ${b.expr}"
      val retStmt = If(Ident(varName), Ident(param.name), Ident(TermName("None")))

      treeCopy.Block(b, b.stats :+ filterCond, retStmt)
    }

  }

}
