package eu.stratosphere.emma.codegen.cogadb

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._
import internal.reificationSupport._

object UDFCodeGenerator extends AnnotatedC {

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

  private def nextOutputIdentifier: TypeName = freshTypeName("RES_")

  private def visit(tree: Tree, isFinalStmt: Boolean = false)(implicit closure: UDFClosure,
    outputs: ListBuffer[CoGaUDFOutput]): String = {
    tree match {
      case Function(_, body) =>
        visit(body, isFinalStmt)

      case DefDef(_, _, _, _, _, rhs) =>
        visit(rhs, isFinalStmt)

      case Block(Nil, expr) =>
        visit(expr, isFinalStmt)

      case Block(xs :+ x, expr) =>
        xs.flatMap(visit(_)).mkString + visit(x) + visit(expr, true)

      case Apply(typeApply: TypeApply, args: List[Tree]) =>
        generateForTypeApply(typeApply, args, isFinalStmt)

      case app@Apply(sel: Select, args: List[Tree]) =>
        generateForSelectApply(app, sel, args, isFinalStmt)

      case sel: Select =>
        generateForSelect(sel)

      case Assign(lhs: Ident, rhs) =>
        AssignmentStmt(LocalVar(lhs.name.toString), visit(rhs))

      case ValDef(mods, name, tpt: TypeTree, rhs) =>
        AssignmentStmt(VarDef(tpt.tpe.toCPrimitive, name.toString), visit(rhs))

      case If(cond, thenp, elsep: Literal) =>
        IfThenElseStmt(visit(cond), visit(thenp), visit(elsep))

      case If(cond, thenp, _) =>
        IfThenStmt(visit(cond), visit(thenp))

      case ide: Ident =>
        generateForIdent(ide, isFinalStmt)

      case lit@Literal(_) =>
        generateForLiteral(lit, isFinalStmt)

      case _ => throw new IllegalArgumentException(s"Code generation for tree type not supported: ${showRaw(tree)}")
    }
  }

  private def generateForLiteral(lit: Literal, isFinalSmt: Boolean)
    (implicit closure: UDFClosure, outputs: ListBuffer[CoGaUDFOutput]): String = {
    if (isFinalSmt) {
      val outputIde = nextOutputIdentifier
      outputs += new CoGaUDFOutput(outputIde, lit.tpe)
      AssignmentStmt(OutputCol(outputIde.toString), Const(lit.value.tpe, lit.value.value.toString))
    } else {
      Const(lit.value.tpe, lit.value.value.toString)
    }
  }

  private def generateForIdent(ide: Ident, isFinalSmt: Boolean)
    (implicit closure: UDFClosure, outputs: ListBuffer[CoGaUDFOutput]): String = {
    if (isFinalSmt) {
      if (ide.tpe.isScalaBasicType) {

        val outputIde = nextOutputIdentifier
        outputs += new CoGaUDFOutput(outputIde, ide.tpe)
        //if input parameter, otherwise local variable
        if (closure.symbolTable isDefinedAt ide.name.toString) {
          val tableCol = closure.symbolTable(ide.name.toString)
          AssignmentStmt(OutputCol(outputIde.toString), InputCol(tableCol))
        } else {
          AssignmentStmt(OutputCol(outputIde.toString), ide.name.toString)
        }
      } else {
        //currently assume that only input parameter can be complex (UDT)
        //i.e. instantiation of complex type not allowed except as final statement
        ide.tpe.members.filter(!_.isMethod).toList.reverse
        .map { fieldIdentifier =>
          val outputIde = nextOutputIdentifier
          outputs += new CoGaUDFOutput(outputIde, fieldIdentifier.typeSignature)
          val coGaColumn = closure.symbolTable(ide.name.toString + "." +
            fieldIdentifier.name.toString.trim)
          AssignmentStmt(OutputCol(outputIde.toString), InputCol(coGaColumn))
        }.mkString
      }
    } else {
      LocalVar(ide.name.toString)
    }
  }

  private def generateForTypeApply(typeApply: TypeApply, args: List[Tree], isFinalSmt: Boolean)
    (implicit closure: UDFClosure, outputs: ListBuffer[CoGaUDFOutput]): String = {
    //initialization of a Tuple type
    if (isFinalSmt) {
      args.map(arg => {
        val outputIde = nextOutputIdentifier
        outputs += new CoGaUDFOutput(outputIde, arg.tpe)
        AssignmentStmt(OutputCol(outputIde.toString), visit(arg))
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
          val outputIde = nextOutputIdentifier
          outputs += new CoGaUDFOutput(outputIde, arg.tpe)
          AssignmentStmt(OutputCol(outputIde.toString), visit(arg))
        }).mkString
      } else {
        val outputIde = nextOutputIdentifier
        outputs += new CoGaUDFOutput(outputIde, app.tpe)
        AssignmentStmt(OutputCol(outputIde.toString), visit(app))
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

  private def generateForSelect(sel: Select)
    (implicit closure: UDFClosure, outputs: ListBuffer[CoGaUDFOutput]): String = {
    if (isSupportedUnaryMethod(sel.name)) {
      UnaryOp(sel.name.toTermName, visit(sel.qualifier))
    } else if (closure.symbolTable isDefinedAt (sel.toString())) {
      InputCol(closure.symbolTable(sel.toString))
    } else {
      throw new IllegalArgumentException(s"Select ${sel.name.toString} not supported.")
    }
  }

}
