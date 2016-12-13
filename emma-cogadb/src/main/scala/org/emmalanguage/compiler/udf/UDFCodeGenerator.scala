package org.emmalanguage
package compiler.udf

import org.emmalanguage.compiler.udf.common._
import compiler.lang.cogadb.ast._


class UDFCodeGenerator(udfClosure: UDFClosure) {

  def generate: Node = {
    udfClosure.udfType match {
      case UDFType.Map => new MapUDFTransformer(udfClosure.ast, udfClosure.symTbl).transform
      case UDFType.Filter => throw new IllegalArgumentException("Not implemented yet")
      case UDFType.Fold => throw new IllegalArgumentException("Not implemented yet")
    }
  }
  
}
