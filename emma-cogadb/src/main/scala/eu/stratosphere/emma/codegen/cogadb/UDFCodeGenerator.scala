package eu.stratosphere.emma.codegen.cogadb

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._
import internal.reificationSupport._

object UDFCodeGenerator extends AnnotatedC with TypeHelper {

  sealed abstract class UDF(ast: Tree)

  case class MapUDF(ast: Tree) extends UDF(ast)

  case class FilterUDF(ast: Tree) extends UDF(ast)

  sealed abstract class AbstractUDTMapping

  case class UDTMapping(v: Map[Symbol, AbstractUDTMapping]) extends AbstractUDTMapping

  case class CoGaTable(identifier: String, columns: Seq[CoGaColumn]) extends AbstractUDTMapping

  case class CoGaColumn(identifier: String)

  case class CoGaUDFOutput(identifier: TypeName, tpe: Type)

  //TODO introduce UDTMapping
  class UDFClosure {
    val symbolTable = mutable.Map.empty[String, String]
  }

  case class CoGaUDF(udf: String, output: Seq[CoGaUDFOutput])

  def generateFor(udf: UDF, symbolTable: UDFClosure): CoGaUDF = {
    implicit val closure = symbolTable
    implicit val outputs = mutable.ListBuffer.empty[CoGaUDFOutput]
    udf match {
      case MapUDF(ast) => CoGaUDF(visit(ast, true), outputs.result())
      case FilterUDF(ast) => throw new IllegalArgumentException("Not implemented yet")
    }
  }

  private def newUDFOutput(tpe: Type)(implicit outputs: ListBuffer[CoGaUDFOutput]): TypeName = {
    val name = freshTypeName("RES_")
    outputs += new CoGaUDFOutput(name, tpe)
    name
  }

  private def visit(tree: Tree, isPotentialOut: Boolean = false)(implicit closure: UDFClosure,
    outputs: ListBuffer[CoGaUDFOutput]): String = {
    tree match {
      case Function(_, body) =>
        visit(body, isPotentialOut)

      case DefDef(_, _, _, _, _, rhs) =>
        visit(rhs, isPotentialOut)

      case Block(Nil, expr) =>
        visit(expr, isPotentialOut)

      case Block(xs, expr) =>
        xs.flatMap(visit(_)).mkString + visit(expr, isPotentialOut)

      case Apply(typeApply: TypeApply, args: List[Tree]) =>
        generateForTypeApply(typeApply, args, isPotentialOut)

      case app@Apply(sel: Select, args: List[Tree]) =>
        generateForSelectApply(app, sel, args, isPotentialOut)

      case sel: Select =>
        generateForSelect(sel, isPotentialOut)

      case Assign(lhs: Ident, rhs) =>
        AssignmentStmt(LocalVar(lhs.name), visit(rhs))

      case ValDef(mods, name, tpt: TypeTree, rhs) =>
        AssignmentStmt(VarDef(tpt.tpe.toCPrimitive, name), visit(rhs))

      //TODO rewrite tree for if then else
      case If(cond, thenp, elsep) =>
        IfThenElseStmt(visit(cond), visit(thenp, isPotentialOut), visit(elsep, isPotentialOut))

      case ide: Ident =>
        generateForIdent(ide, isPotentialOut)

      case lit@Literal(_) =>
        generateForLiteral(lit, isPotentialOut)

      case _ => throw new IllegalArgumentException(s"Code generation for tree type not supported: ${showRaw(tree)}")
    }
  }

  private def generateForLiteral(lit: Literal, isFinalStmt: Boolean)
    (implicit closure: UDFClosure, outputs: ListBuffer[CoGaUDFOutput]): String = {
    if (isFinalStmt) {
      AssignmentStmt(OutputCol(newUDFOutput(lit.tpe)), Const(lit.value))
    } else {
      Const(lit.value)
    }
  }

  private def generateForIdent(ide: Ident, isFinalStmt: Boolean)
    (implicit closure: UDFClosure, outputs: ListBuffer[CoGaUDFOutput]): String = {
    if (isFinalStmt) {
      if (ide.tpe.isScalaBasicType) {
        //if input parameter, otherwise local variable
        if (closure.symbolTable isDefinedAt ide.name.toString) {
          val tableCol = closure.symbolTable(ide.name.toString)
          AssignmentStmt(OutputCol(newUDFOutput(ide.tpe)), InputCol(tableCol))
        } else {
          AssignmentStmt(OutputCol(newUDFOutput(ide.tpe)), ide.name.toString)
        }
      } else {
        //currently assume that only input parameter can be complex (UDT)
        //i.e. instantiation of complex type not allowed except as final statement
        ide.tpe.members.filter(!_.isMethod).toList.reverse
        .map { fieldIdentifier =>
          val coGaColumn = closure.symbolTable(ide.name.toString + "." +
            fieldIdentifier.name.toString.trim)
          AssignmentStmt(OutputCol(newUDFOutput(ide.tpe)), InputCol(coGaColumn))
        }.mkString
      }
    } else {
      LocalVar(ide.name)
    }
  }

  private def generateForTypeApply(typeApply: TypeApply, args: List[Tree], isFinalSmt: Boolean)
    (implicit closure: UDFClosure, outputs: ListBuffer[CoGaUDFOutput]): String = {
    //initialization of a Tuple type
    if (isFinalSmt) {
      args.map(arg => {
        AssignmentStmt(OutputCol(newUDFOutput(arg.tpe)), visit(arg))
      }).mkString
    } else {
      throw new IllegalArgumentException(s"Instantiation of ${typeApply.toString()} not allowed at this place.")
    }
  }

  private def generateForSelectApply(app: Apply, sel: Select, args: List[Tree], isFinalSmt: Boolean)
    (implicit closure: UDFClosure, outputs: ListBuffer[CoGaUDFOutput]): String = {
    if (isFinalSmt) {
      if (isInstantiation(sel.name)) {
        //initialization of a complex type
        args.map(arg => {
          AssignmentStmt(OutputCol(newUDFOutput(arg.tpe)), visit(arg))
        }).mkString
      } else {
        AssignmentStmt(OutputCol(newUDFOutput(app.tpe)), visit(app))
      }
    } else {
      if (isSupportedBinaryMethod(sel.name)) {
        BinaryOp(sel.name.toTermName, visit(sel.qualifier), visit(args.head))
      } else if (isSupportedLibrary(sel.qualifier)) {
        args.size match {
          case 1 => UnaryOp(sel.name.toTermName, visit(args(0)))
          case 2 => BinaryOp(sel.name.toTermName, visit(args(0)), visit(args(1)))
          case _ => throw new IllegalArgumentException(s"Select ${sel.name.toString} with ${args.size} arguments " +
            "not supported.")
        }
      } else {
        throw new IllegalArgumentException(s"Apply ${sel.name.toString} not supported.")
      }
    }
  }

  private def generateForSelect(sel: Select, isFinalStmt: Boolean)
    (implicit closure: UDFClosure, outputs: ListBuffer[CoGaUDFOutput]): String = {
    if (isSupportedUnaryMethod(sel.name)) {
      val op = UnaryOp(sel.name.toTermName, visit(sel.qualifier))
      if (isFinalStmt)
        AssignmentStmt(OutputCol(newUDFOutput(sel.tpe)), UnaryOp(sel.name.toTermName, visit(sel.qualifier)))
      else
        NoOp
    } else if (closure.symbolTable isDefinedAt (sel.toString())) {
      if (isFinalStmt)
        AssignmentStmt(OutputCol(newUDFOutput(sel.tpe)), InputCol(closure.symbolTable(sel.toString)))
      else
        NoOp
    } else {
      throw new IllegalArgumentException(s"Select ${sel.toString} not supported.")
    }
  }

}
