package eu.stratosphere.emma.codegen.cogadb.udfcompilation

import eu.stratosphere.emma.codegen.cogadb.udfcompilation.common._
import eu.stratosphere.emma.codegen.cogadb.udfcompilation.common.UDFType.UDFType

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

object UDFCompiler {

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  def compile(udfCode: String, udfType: UDFType, symbolTable: Map[String, String]): Unit = {
    val ast = tb.typecheck(q"$udfCode")
    val codeGenerator = new UDFCodeGenerator(UDFClosure(ast, symbolTable, udfType))
    codeGenerator.generate
  }
}
