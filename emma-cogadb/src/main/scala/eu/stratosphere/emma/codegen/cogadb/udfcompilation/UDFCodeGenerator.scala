package eu.stratosphere.emma.codegen.cogadb.udfcompilation

import eu.stratosphere.emma.codegen.cogadb.udfcompilation.common._

class UDFCodeGenerator(udfClosure: UDFClosure) {

  def generate: TransformedUDF = {
    udfClosure.udfType match {
      case UDFType.Map => generateForMapUDF
      case UDFType.Filter => throw new IllegalArgumentException("Not implemented yet")
      case UDFType.Fold => throw new IllegalArgumentException("Not implemented yet")
    }
  }
  
  private def generateForMapUDF = {
    val mapTransformer = new MapUDFTransformer()
    mapTransformer.transform(udfClosure.ast, udfClosure.symTbl)
  }

}
