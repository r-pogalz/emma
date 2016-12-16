package org.emmalanguage.compiler.udf

import org.emmalanguage.compiler.lang.cogadb.ast._
import org.emmalanguage.compiler.udf.common._

import scala.collection.mutable
import scala.reflect.runtime.universe._
import internal.reificationSupport._
import scala.tools.reflect.ToolBox

/**
  * Currently we only support transformation of reductions on primitive types
  *
  * @param zAst
  * @param sngAst
  * @param uniAst
  * @param symbolTable
  * @return
  */
class ReduceUDFGenerator(zAst: Tree, sngAst: Tree, uniAst: Tree, symbolTable: Map[String, String])
  extends JsonIRGenerator[AlgebraicReduceUdf] with AnnotatedCCodeGenerator with TypeHelper {

  private val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  val intermediateReduceCodeResult = "intermediate_reduce_res"

  def generate: AlgebraicReduceUdf = zAst match {
    case Literal(c: Constant) => {
      val redUdfPayload = ReduceUdfAttr(c.tpe.toJsonAttributeType, intermediateReduceCodeResult, matchConst(c))

      //transform sngAst and merge with uniAst
      val localMapVarName = "local_map_res"
      val sngTransformedAst = {
        val transformed = new SngUDFTransformer(localMapVarName, c.tpe).transform(sngAst)
        val unTypedChecked = tb.untypecheck(transformed)
        tb.typecheck(unTypedChecked)
      }

      val uniTransformedAst = {
        val uniTransformer = new UniUDFTransformer(intermediateReduceCodeResult, localMapVarName)
        uniTransformer.transform(uniAst)
        uniTransformer.resultBody
      }

      val mergedAsts = {
        val transformed = new SngUniAstMerger(intermediateReduceCodeResult, c.tpe,
          uniTransformedAst).transform(sngTransformedAst)
        val unTypedChecked = tb.untypecheck(transformed)
        tb.typecheck(unTypedChecked)
      }
      
      val updatedSymTbl = symbolTable ++ Map(intermediateReduceCodeResult -> "<hash_entry>")
      val reduceUdfCode: Seq[String] = generateAnnotatedCCode(updatedSymTbl, mergedAsts, true)
      
      val finalReduceOutName = freshTypeName("REDUCE_UDF_RES_")
      val finalReduceOutAttr = ReduceUdfOutAttr(c.tpe.toJsonAttributeType, s"$finalReduceOutName",
        s"$finalReduceOutName")
      val reduceUdfFinalCode =
        ReduceUdfCode(s"#<OUT>.${finalReduceOutName}#=#<hash_entry>.$intermediateReduceCodeResult#;")
      
      AlgebraicReduceUdf(Seq(redUdfPayload), Seq(finalReduceOutAttr), reduceUdfCode.map(ReduceUdfCode(_)),
        Seq(reduceUdfFinalCode))
    }
    case _ => throw new IllegalArgumentException(s"First input parameter for fold UDF compilation must be a Literal.")
  }

  private class SngUniAstMerger(intermediateResultName: String, redOutTpe: Type, uniBodyAst: Tree) extends Transformer {

    override def transform(tree: Tree) = tree match {
      case fun@Function(_, body: Function) => super.transform(fun)
      case fun@Function(vparams, body) => {
        val udfHeadParam = q" val ${TermName(intermediateResultName)}: ${redOutTpe} = ${EmptyTree}"
        treeCopy.Function(fun, udfHeadParam.asInstanceOf[ValDef] +: vparams, merge(body))
      }
      case t => super.transform(t)
    }

    def merge(tree: Tree): Tree = tree match {
      case b: Block => mergeWithSngBlock(b)
      case t => mergeWithSngSingleExpr(t)
    }

    def mergeWithSngBlock(sngBlock: Block): Block = uniBodyAst match {
      case uniBlock: Block =>
        treeCopy.Block(sngBlock, sngBlock.stats ++ List(sngBlock.expr) ++ uniBlock.stats, uniBlock.expr)
      case t => treeCopy.Block(sngBlock, sngBlock.stats ++ List(sngBlock.expr), t)
    }

    def mergeWithSngSingleExpr(sngExpr: Tree): Tree = uniBodyAst match {
      case uniBlock: Block => q"{..${List(sngExpr) ++ uniBlock.stats ++ List(uniBlock.expr)}}"
      case t => q"{..${List(sngExpr) ++ List(t)}}"
    }
  }
  
  private class SngUDFTransformer(localMapVarName: String, redOutTpe: Type) extends Transformer {

    override def transform(tree: Tree) = tree match {
      case fun@Function(_, body: Function) => super.transform(fun)
      case fun@Function(vparams, body) => treeCopy.Function(fun, vparams, rewrite(body))
      case t => super.transform(t)
    }

    def rewrite(tree: Tree): Tree = tree match {
      case b: Block => transformBlock(b)
      case t => transformSingleExpr(t)
    }

    def transformBlock(b: Block): Block = {
      val retStmt = q" val ${TermName(localMapVarName)}: ${redOutTpe} = ${b.expr}"

      treeCopy.Block(b, b.stats, retStmt)
    }

    def transformSingleExpr(rhs: Tree): Tree = q" val ${TermName(localMapVarName)}: ${redOutTpe} = ${rhs}"
  }

  
  private class UniUDFTransformer(intermediateResName: String, localMapVarName: String) extends Transformer {

    private var toReplaceFirst: TermName = TermName("")
    private var toReplaceSecond: TermName = TermName("")
    var resultBody: Tree = EmptyTree

    override def transform(tree: Tree) = tree match {
      case fun@Function(_, body: Function) => super.transform(fun)
      case fun@Function(vparams, body) => {
        toReplaceFirst = vparams.head.name
        toReplaceSecond = vparams.last.name
        resultBody = super.transform(body)
        treeCopy.Function(fun, vparams, resultBody)
      }
      case ide@Ident(term: TermName) =>
          if (s"$toReplaceFirst" == s"$term") Ident(TermName(intermediateResName))
          else if (s"$toReplaceSecond" == s"$term") Ident(TermName(localMapVarName)) else ide
      case t => super.transform(t)
    }
  }
  
  override def generateLocalVar(name: Name): String = if(s"$name" != intermediateReduceCodeResult) s"$name"
                                                      else s"#<hash_entry>.$intermediateReduceCodeResult#"

  override def generateOutputExpr(col: TypeName): String = s"#<hash_entry>.$col#"

  override protected def newUDFOutput(tpe: Type, infix: String): TypeName = TypeName(intermediateReduceCodeResult)

  override protected def basicTypeColumnIdentifier: String = intermediateReduceCodeResult

  override protected def freshVarName: TermName = freshTermName("reduce_udf_local_var_")
}
