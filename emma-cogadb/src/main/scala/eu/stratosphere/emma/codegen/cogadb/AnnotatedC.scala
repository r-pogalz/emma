package eu.stratosphere.emma.codegen.cogadb

import scala.reflect.runtime.universe._

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

  def isSupportedUnaryMethod(name: Name): Boolean =
    if(name.isTermName) supportedUnaryMethods contains name.toTermName else false
  
  def isSupportedBinaryMethod(name: Name): Boolean =
    if (name.isTermName) supportedBinaryMethods contains name.toTermName else false


  def isSupportedLibrary(qualifier: Tree): Boolean = supportedLibraries contains qualifier.toString
  
  def isInstantiation(name: Name): Boolean = name == TermName("apply")

  def Const(const: Constant): String =
    if (const.tpe == typeOf[String]) "\""+ const.value +"\""
    else if (const.tpe == typeOf[Char]) "\'" + const.value + "\'"
    else s"${const.value}"

  def LocalVar(name: Name): String = s"$name"
  
  def VarDef(tpe: Name, v: Name): String = s"$tpe $v"
  
  def InputCol(tableCol: String): String = s"#$tableCol#"
  
  def OutputCol(col: TypeName): String = s"#<OUT>.$col#"
  
  def NoOp: String = ""
  
  def UnaryOp(op: Name, arg: String): String = supportedUnaryMethods get op.toTermName match {
    case Some(o) => s"$o($arg)"
    case None => s"$op($arg)"
  }
  
  def BinaryOp(op: Name, arg1: String, arg2: String): String = supportedBinaryMethods get op.toTermName match {
    case Some(o) => s"($arg1${o.name}$arg2)"
    case None => s"$op($arg1,$arg2)"
  }
  
  def AssignmentStmt(lhs: String, rhs: String): String = s"$lhs=$rhs;"
  
  def IfThenElseStmt(cond: String, thenp: String, elsep: String): String = s"if($cond){$thenp}else{$elsep}"
  
  
}
