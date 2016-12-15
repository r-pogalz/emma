package org.emmalanguage
package compiler.udf

import org.emmalanguage.compiler.udf.common._
import compiler.lang.cogadb.ast._

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox


class UDFCodeGenerator(udfClosure: UDFClosure) {

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  def generate: Node = udfClosure.ast match {
    case fun: Function => udfClosure.udfType match {
      case UDFType.Map => new MapUDFTransformer(fun, udfClosure.symTbl).transform
      case UDFType.Filter => generateForFilterUDF(fun)
      case UDFType.Fold => throw new IllegalArgumentException("Not implemented yet")
    }
    case _ => throw new IllegalArgumentException(s"Scala AST is not a Function: ${showRaw(udfClosure.ast)}")
  }

  private def generateForFilterUDF(fun: Function): Node = {
    genericSelectionChecker.traverse(fun)
    if (genericSelectionChecker.isTransformable) {
      //generate GENERIC_SELECTION predicate
      new SelectionTransformer(udfClosure.ast, udfClosure.symTbl).transform
    } else {
      //rewrite filter predicate to flatMap UDF and apply map udf transformation
      val flatMapUdf = filterToFlatMapUDFTransformer.transform(fun)
      val unTypeCheckedAST = tb.untypecheck(flatMapUdf)
      val typeCheckedAST = tb.typecheck(unTypeCheckedAST)

      new MapUDFTransformer(typeCheckedAST, udfClosure.symTbl).transform
    }
  }

  /**
    * implements heuristic to check if a filter UDF can be transformed to GENERIC_SELECTION
    * filter UDF must not be a Block and should not contain unsupported comparator functions
    */
  private object genericSelectionChecker extends Traverser {

    var isTransformable = true
    private val supportedSelectionOperations = Seq(
      TermName("$eq$eq"),       //==
      TermName("$less"),        //<
      TermName("$less$eq"),     //<=
      TermName("$greater"),     //>
      TermName("$greater$eq"),  //>=
      TermName("$amp$amp"),     //&&
      TermName("$bar$bar")      //||
    )

    override def traverse(tree: Tree) = tree match {
      case b: Block => isTransformable = false
      case Apply(sel: Select, args: List[Tree]) => if (!isSupportedComparator(sel.name)) isTransformable = false
                                                   else super.traverseTrees(sel +: args)
      case t => super.traverse(t)
    }

    private def isSupportedComparator(name: Name): Boolean =
      if (name.isTermName) supportedSelectionOperations contains name.toTermName else false
  }

  private object filterToFlatMapUDFTransformer extends Transformer {
    override def transform(tree: Tree) = tree match {
      case fun@Function(vparams, body) =>
        treeCopy.Function(fun, vparams, rewrite(tree, body, vparams))
      case t => super.transform(t)
    }

    def rewrite(parent: Tree, t: Tree, params: List[ValDef]): Tree = t match {
      case b: Block => transformBlock(b, params.head)
      case app: Apply => transformApply(app, params.head)
      case _ => super.transform(parent)
    }

    def transformBlock(b: Block, udfParam: ValDef): Block = {
      val varName = TermName("filter_udf_condition")
      val filterCond = q" val $varName: Boolean = ${b.expr}"
      val retStmt = If(Ident(varName), Ident(udfParam.name), Ident(TermName("None")))

      treeCopy.Block(b, b.stats :+ filterCond, retStmt)
    }

    def transformApply(app: Apply, udfParam: ValDef): Tree = {
      val varName = TermName("filter_udf_condition")
      val filterCond = q" val $varName: Boolean = ${app}"
      val retStmt = If(Ident(varName), Ident(udfParam.name), Ident(TermName("None")))
      q"{..${filterCond :: retStmt :: List.empty[Tree]}}"
    }

  }

}
