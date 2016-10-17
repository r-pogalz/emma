package eu.stratosphere.emma.codegen.cogadb

import eu.stratosphere.emma.codegen.cogadb.AOperator.AOperator
import eu.stratosphere.emma.codegen.cogadb.Type.Type
import scala.reflect.runtime.universe._

sealed abstract class Stmt

sealed abstract class Expr

case class StmtBlock(stmts: List[Stmt]) extends Stmt

case class IfThen(cond: Expr, thenStmt: Stmt) extends Stmt

case class IfThenElse(cond: Expr, thenStmt: Stmt, elseStmt: Stmt) extends Stmt

//
//case class ForLoop(inits: Stmt, check: Expr, inc: Stmt, block: Stmt) extends Stmt
//
//case class WhileLoop(check: Expr, block: Stmt) extends Stmt
//
//case class VarDecl(name: String, tpe: Type) extends Stmt

case class Assignment(op: AOperator, leftVal: Var, rightVal: Expr) extends Stmt

case class NoOp(expr: Expr) extends Stmt

abstract class Var extends Expr

case class InputCol(name: String) extends Var

case class OutputCol(name: String) extends Var

case class VarDef(name: String, tpe: Type) extends Var

case class LocalVar(name: String) extends Var

case class Const(value: String, tpe: Type) extends Expr

abstract class Operator extends Expr

case class UnaryOp(op: String, operand: Expr) extends Expr

case class BinaryOp(op: String, left: Expr, right: Expr) extends Operator

object CoGaExpr extends Enumeration {
  type CoGaExpr = Value
  val NoOp = Value("%s;")
  val Assignment = Value("%s=%s;")
  val LocalVar = Value("%s")
  val VarDef = Value("")
  val Const = Value("")
  val CoGaInputCol = Value("#%s#")
  val CoGaOutputCol = Value("#<OUT>.%s#")
  val UnaryOperation = Value("")
  val BinaryOperation = Value("")
  val IfThen = Value("if(%s){%s}")
  val IfThenElse = Value("if(%s){%s}else{%s}")

  def toCode(exprType: CoGaExpr, args: String*) = exprType match {
    case NoOp | LocalVar | CoGaOutputCol => exprType.toString.format(args(0))
    case UnaryOperation => args(0).toString.format(args(1))
    case BinaryOperation => args(0).toString.format(args(1), args(2))
    case Assignment | IfThen => exprType.toString.format(args(0), args(1))
    case IfThenElse => exprType.toString.format(args(0), args(1), args(2))
    case VarDef => args(0).format(args(1))
    case CoGaInputCol => exprType.toString.format(args(0).toUpperCase)
    case Const => args(0) match {
      case "String" => s"${"\"" + args(1) + "\""}"
      case "Char" => s"'${args(1)}'"
      case _ => args(1)
    }
  }
}

object Type extends Enumeration {
  type Type = Value
  val CHAR = Value("char %s")
  val SHORT = Value("short %s")
  val INT = Value("int32_t %s")
  val LONG = Value("int64_t %s")
  val FLOAT = Value("float %s")
  val DOUBLE = Value("double %s")
  val STRING = Value("char %s[]")
  val BOOLEAN = Value("bool %s")
  val ANY = Value("")

  def fromScalaType(tpe: String) = tpe match {
    case "Int" => INT
    case "Long" => LONG
    case "Float" => FLOAT
    case "Double" => DOUBLE
    case "String" | "java.lang.String" => STRING
    case "Char" => CHAR
    case "Boolean" => BOOLEAN
    case "AnyVal" => ANY
    case _ => throw new IllegalArgumentException(s"Type $tpe not supported.")
  }
  
  def isScalaBasicType(tpe: String) = tpe match {
    case "Int" | "Long" | "Float" | "Double" | "String" | "java.lang.String" | "Char" | "Boolean" | "Byte" => true
    case _ => false
  }
}

object AOperator extends Enumeration {
  type AOperator = Value
  val Eq = Value("=")
  val PlusEq = Value("+=")
  val MinusEq = Value("-=")
}

object UOperator {
  private val unaryOps = collection.immutable.HashMap("toDouble" -> "(double)%s", "toFloat" -> "(float)%s",
    "toInt" -> "(int23_t)%s", "toLong" -> "(int64_t)%s")

  def fromScalaOp(sel: Select) = {
    if (sel.qualifier.toString == "scala.math.`package`") {
      s"${sel.name.toString}(%s)"
    } else if (unaryOps isDefinedAt sel.name.toString) {
      unaryOps(sel.name.toString)
    } else {
      throw new IllegalArgumentException(s"Unary operator ${sel.toString()} not supported.")
    }
  }
}

object BOperator {
  //  type BOperator = Value
  //  val Plus = Value("%s+%s")
  //  val StrConcat = Value("%s %s")
  //  val Minus = Value("%s-%s")
  //  val Times = Value("%s*%s")
  //  val Div = Value("%s/%s")
  //  val Mod = Value("%s%%s")
  //  val EqEq = Value("%s==%s")
  //  val LessThen = Value("%s<%s")
  //  val GreaterThen = Value("%s>%s")
  //  val LessEq = Value("%s<=%s")
  //  val GreaterEq = Value("%s>=%s")
  //  val Xor = Value("%s^%s")
  //  val Pow = Value("pow(%s,%s)")

  private val binaryOps = collection.immutable.HashMap("$plus" -> "(%s+%s)",
    "$minus" -> "(%s-%s)", "$times" -> "(%s*%s)", "$div" -> "(%s/%s)", "$eq$eq" -> "%s==%s", "$up" -> "(%s^%s)")
  
  def fromScalaOp(sel: Select, resultType: String) = {
    val funName = sel.name.toString
    if (sel.qualifier.toString == "scala.math.`package`") {
      s"$funName(%s,%s)"
    } else if (binaryOps isDefinedAt funName) {
      //internal ops
      funName match {
        case "$plus" => resultType match {
          case "String" | "java.lang.String" => "%s %s"
          case _ => binaryOps(funName)
        }
        case "$up" => resultType match {
          //use math lib function for 2^1 -> pow(2,1)
          case "Int" => s"$funName(%s,%s)"
          case _ => binaryOps(funName)
        }
        case _ => binaryOps(funName)
      }
    } else {
      throw new IllegalArgumentException(s"Binary operator ${sel.toString()} not supported.")
    }
  }
}