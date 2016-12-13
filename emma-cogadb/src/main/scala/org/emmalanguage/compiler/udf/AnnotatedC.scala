package org.emmalanguage
package compiler.udf


import scala.reflect.runtime.universe._
import internal.reificationSupport._

trait AnnotatedC {

  private val supportedUnaryMethods = Map(
    TermName("toDouble") -> "(double)",
    TermName("toFloat") -> "(float)",
    TermName("toInt") -> "(int32_t)",
    TermName("toLong") -> "(int64_t)"
  )

  private val supportedBinaryMethods = Map(
    TermName("$plus") -> '+,
    TermName("$minus") -> '-,
    TermName("$times") -> '*,
    TermName("$div") -> '/,
    //    TermName("$up") -> '^,
    TermName("$eq$eq") -> '==,
    TermName("$less") -> '<,
    TermName("$less$eq") -> '<=,
    TermName("$greater") -> '>,
    TermName("$greater$eq") -> '>=
  )

  private val supportedLibraries = Seq(
    "scala.math.`package`"
  )

  def newUDFOutput(tpe: Type, prefix: String): TypeName = {
    val name = freshTypeName(prefix)
    name
  }

  def isSupportedUnaryMethod(name: Name): Boolean =
    if (name.isTermName) supportedUnaryMethods contains name.toTermName else false

  def isSupportedBinaryMethod(name: Name): Boolean =
    if (name.isTermName) supportedBinaryMethods contains name.toTermName else false


  def isSupportedLibrary(qualifier: Tree): Boolean = supportedLibraries contains qualifier.toString

  def isInstantiation(name: Name): Boolean = name == TermName("apply")

  def Const(const: Constant): String =
    if (const.tpe == typeOf[String]) "\"" + const.value + "\""
    else if (const.tpe == typeOf[Char]) "\'" + const.value + "\'"
    else s"${const.value}"

  def generateLocalVar(name: Name): String = s"$name"

  def generateLineStmt(stmt: String): String = s"$stmt;"

  def generateVarDef(tpe: Name, v: Name): String = s"$tpe $v"

  def generateColAccess(tableCol: String): String = s"#$tableCol#"

  def generateOutputExpr(col: TypeName): String = s"#<OUT>.$col#"

  def generateNoOp: String = ""

  def generateUnaryOp(op: Name, arg: String): String = supportedUnaryMethods get op.toTermName match {
    case Some(o) => s"$o($arg)"
    case None => s"$op($arg)"
  }

  def generateBinaryOp(op: Name, arg1: String, arg2: String): String = supportedBinaryMethods get op.toTermName match {
    case Some(o) => s"($arg1${o.name}$arg2)"
    case None => s"$op($arg1,$arg2)"
  }

  def generateAssignmentStmt(lhs: String, rhs: String): Seq[String] = Seq(generateLineStmt(s"$lhs=$rhs"))

  def generateIfThenElseStmt(cond: String, thenp: Seq[String], elsep: Seq[String]): Seq[String] =
    Seq(s"if($cond){") ++ thenp ++ Seq("}else{") ++ elsep ++ Seq("}")


}
