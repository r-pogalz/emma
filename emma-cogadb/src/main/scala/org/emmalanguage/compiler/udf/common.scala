package org.emmalanguage
package compiler.udf

import compiler.lang.cogadb.ast._
import org.emmalanguage.compiler.udf.common.UDFType.UDFType

import scala.reflect.runtime.universe._

object common {

  object UDFType extends Enumeration {
    type UDFType = Value
    val Map, Filter, Fold = Value
  }

  case class UDFClosure(ast: Tree, symTbl: Map[String, String], udfType: UDFType)

  trait UDFTransformer[Ret <: Node] {
    protected def transform: Ret
  }

}
