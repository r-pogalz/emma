package eu.stratosphere.emma.codegen.cogadb

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

    private val basicTypes = Seq(typeOf[Short], typeOf[Int], typeOf[Long], typeOf[Float], typeOf[Double], typeOf[Char],
      typeOf[String], typeOf[java.lang.String], typeOf[Boolean])
    
    def isValidUDT: Boolean = ???

    def toCPrimitive: Name = scalaToC.get(t) match {
      case Some(value) => value
      case None => throw new IllegalArgumentException(s"No existing primitive C type for $t.")
    }

    def isScalaBasicType: Boolean = basicTypes.contains(t)
  }


}
