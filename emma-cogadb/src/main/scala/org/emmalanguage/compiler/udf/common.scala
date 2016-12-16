package org.emmalanguage
package compiler.udf

import compiler.lang.cogadb.ast._

import scala.reflect.runtime.universe._

object common {

  sealed abstract class UDFClosure(symTbl: Map[String, String])
  
  case class MapUDFClosure(ast: Tree, symTbl: Map[String, String]) extends UDFClosure(symTbl)

  case class FilterUDFClosure(ast: Tree, symTbl: Map[String, String]) extends UDFClosure(symTbl)
  
  case class FoldUDFClosure(zElem: Tree, sngAst: Tree, uniAst: Tree, symTbl: Map[String, String])
    extends UDFClosure(symTbl)

  trait JsonIRGenerator[Ret <: Node] {
    protected def generate: Ret
  }

  def matchConst(c: Constant): Const =
    if (c.tpe == typeOf[Short] || c.tpe == typeOf[Int])
      IntConst(c.value.asInstanceOf[Int])
    else if (c.tpe == typeOf[Float])
           FloatConst(c.value.asInstanceOf[Float])
    else if (c.tpe == typeOf[Double])
           DoubleConst(c.value.asInstanceOf[Double])
    else if (c.tpe == typeOf[String] || c.tpe == typeOf[java.lang.String])
           VarCharConst(c.value.asInstanceOf[String])
    else if (c.tpe == typeOf[Char])
           CharConst(c.value.asInstanceOf[Char])
    else if (c.tpe == typeOf[Boolean])
           BoolConst(c.value.asInstanceOf[String])
    else
      throw new IllegalArgumentException(s"Constant $c of type ${c.tpe} not supported.")

}
