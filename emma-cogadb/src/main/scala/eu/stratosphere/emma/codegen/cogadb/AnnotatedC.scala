package eu.stratosphere.emma.codegen.cogadb

import scala.reflect.runtime.universe._

trait AnnotatedC extends UDTHelper {
  
  private val supportedUnaryMethods = Map(
    TermName("toDouble") -> "(double)",
    TermName("toFloat") -> "(float)",
    TermName("toInt") -> "(int32_t)",
    TermName("toLong") -> "(int64_t)")

  private val supportedBinaryMethods = Map(
    TermName("$plus") -> '+,
    TermName("$minus") -> '-,
    TermName("$times") -> '*,
    TermName("$div") -> '/,
//    TermName("$up") -> '^,
    TermName("$eq$eq") -> '==
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

  def Const(tpe: Type, const: String): String =
    if (tpe == typeOf[String]) "\""+ const +"\"" else if (tpe == typeOf[Char]) "\'" + const + "\'" else const

  def LocalVar(v: String): String = v
  
  def VarDef(tpe: TypeName, v: String): String = s"$tpe $v"
  
  def InputCol(tableCol: String): String = s"#$tableCol#"
  
  def OutputCol(col: String): String = s"#<OUT>.$col#"
  
  def UnaryOp(op: TermName, arg: String): String = supportedUnaryMethods get op match {
    case Some(o) => s"$o($arg)"
    case None => s"$op($arg)"
  }
  
  def BinaryOp(op: TermName, arg1: String, arg2: String): String = supportedBinaryMethods get op match {
    case Some(o) => s"($arg1${o.name}$arg2)"
    case None => s"$op($arg1,$arg2)"
  }
  
  def AssignmentStmt(lhs: String, rhs: String): String = s"$lhs=$rhs;"
  
  def IfThenStmt(cond: String, thenp: String): String = s"if($cond){$thenp}"
  
  def IfThenElseStmt(cond: String, thenp: String, elsep: String): String = s"if($cond){$thenp}else{$elsep}"
  
  
}
