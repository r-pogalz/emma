package eu.stratosphere.emma.codegen.helper

import scala.reflect.runtime.universe._

object TypeExtractor {

  def extractNameFrom(tpt: TypeTree) = {
    val classPattern = "class ([^']*)".r
    classPattern findFirstIn tpt.symbol.toString match {
      case Some(classPattern(name)) => name
      case _ => throw new IllegalArgumentException("No input class name found.")
    }
  }
}
