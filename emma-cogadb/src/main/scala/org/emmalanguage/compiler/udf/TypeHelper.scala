package org.emmalanguage
package compiler.udf

import scala.language.implicitConversions
import scala.reflect.runtime.universe._

trait TypeHelper {

  implicit def typeWrapper(t: Type) = new TypeHelperClass(t)

  final class TypeHelperClass(t: Type) {

    private val scalaToC = Map(
      typeOf[Short] -> TypeName("int16_t"),
      typeOf[Int] -> TypeName("int32_t"),
      typeOf[Long] -> TypeName("int64_t"),
      typeOf[Float] -> TypeName("float"),
      typeOf[Double] -> TypeName("double"),
      typeOf[Char] -> TypeName("char"),
      typeOf[String] -> TypeName("char*"),
      typeOf[java.lang.String] -> TypeName("char*"),
      typeOf[Boolean] -> TypeName("bool"))

    private val scalaToJsonAttributeType = Map(
      typeOf[Short] -> "INT",
      typeOf[Int] -> "INT",
      typeOf[Long] -> "UINT32", //this does not work for negative values, ask Sebastian Bress to support Long
      typeOf[Float] -> "FLOAT",
      typeOf[Double] -> "DOUBLE",
      typeOf[Char] -> "CHAR",
      typeOf[String] -> "VARCHAR",
      typeOf[java.lang.String] -> "VARCHAR",
      typeOf[Boolean] -> "BOOLEAN")

    private val basicTypes = Seq(typeOf[Short], typeOf[Int], typeOf[Long], typeOf[Float], typeOf[Double], typeOf[Char],
      typeOf[String], typeOf[java.lang.String], typeOf[Boolean])

    def toCPrimitive: Name = scalaToC.get(t) match {
      case Some(value) => value
      case None => throw new IllegalArgumentException(s"No existing primitive C type for $t.")
    }

    def toJsonAttributeType: String = scalaToJsonAttributeType.get(t) match {
      case Some(value) => value
      case None => throw new IllegalArgumentException(s"No existing JSON attribute type for $t.")
    }

    def isScalaBasicType: Boolean = basicTypes.contains(t)

  }


}
