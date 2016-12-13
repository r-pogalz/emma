package org.emmalanguage
package compiler.udf

import org.emmalanguage.compiler.lang.cogadb.ast._
import org.emmalanguage.compiler.udf.common.UDFType.UDFType
import org.emmalanguage.compiler.udf.common._

import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

object UDFCompiler {

  val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()

  def compile(udfCode: String, udfType: UDFType, symbolTable: Map[String, String]): Node = {
    val ast = tb.typecheck(q"$udfCode")
    val codeGenerator = new UDFCodeGenerator(UDFClosure(ast, symbolTable, udfType))
    codeGenerator.generate
  }
}
