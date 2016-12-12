package eu.stratosphere.emma.codegen.cogadb.udfcompilation

import eu.stratosphere.emma.codegen.cogadb.udfcompilation.common.UDFType.UDFType

import scala.reflect.runtime.universe._

object common {

  object UDFType extends Enumeration {
    type UDFType = Value
    val Map, Filter, Fold = Value
  }

  case class UDFOutput(identifier: TypeName, tpe: Type)

  case class UDFClosure(ast: Tree, symTbl: Map[String, String], udfType: UDFType)

  case class TransformedUDF(udf: String, output: Seq[UDFOutput])

  trait UDFTransformer {
    protected def transform(ast: Tree, symTbl: Map[String, String]): TransformedUDF
  }

}
