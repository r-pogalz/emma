package eu.stratosphere.emma.codegen.cogadb

import eu.stratosphere.emma.codegen.cogadb.CoGaExpr.toCode
import eu.stratosphere.emma.codegen.helper.TypeExtractor

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._

object CoGaUDFCompiler {

  private var outputId = 0

  def lastOutputId = outputId

  case class CoGaUDFOutput(identifier: String, tpe: Type)

  def compile(udf: ScalaUDF, symbolTable: UDFClosure): CoGaUDF = {
    implicit val closure = symbolTable
    implicit val outputs = mutable.ListBuffer.empty[CoGaUDFOutput]
    udf match {
      case ScalaMapUDF(ast) => CoGaUDF(compileBody(ast, true), outputs.result())
      case ScalaFilterUDF(ast) => throw new IllegalArgumentException("Not implemented yet")
    }
  }

  @deprecated
  def extractInputParams(tree: Tree): List[ValDef] = {
    tree match {
      case fun@Function(_,
      body: Function) => {
        body match {
          case Function(vparams,
          _) => {
            vparams
          }
          case _ => throw new IllegalArgumentException(s"No UDF found: ${showRaw(tree)}")
        }
      }
      case _ => throw new IllegalArgumentException(s"Entry tree type not supported: ${showRaw(tree)}")
    }
  }

  @deprecated
  private def compileUDF(tree: Tree)
    (implicit closure: UDFClosure,
      outputs: ListBuffer[CoGaUDFOutput]): String = {
    println("----------------- Raw tree ---------------------")
    println(showRaw(tree))

    tree match {
      case fun@Function(_,
      body: Function) => {
        body match {
          case Function(vparams,
          body) => {
            //            val context = InputTypeMapper.map(vparams)
            compileBody(body,
              true)
          }
          case _ => throw new IllegalArgumentException(s"No UDF found: ${showRaw(tree)}")
        }
      }
      case _ => throw new IllegalArgumentException(s"Entry tree type not supported: ${showRaw(tree)}")
    }
  }

  private def nextOutputIdentifier(): String = {
    outputId += 1
    s"RES_$outputId"
  }

  private def compileBody(tree: Tree,
    potentialOutput: Boolean)
    (implicit closure: UDFClosure,
      outputs: ListBuffer[CoGaUDFOutput]): String = {
    tree match {
      case Function(vparams, body) => compileBody(body, potentialOutput)
      case DefDef(mods, name, tparams, vparamss, tpt, rhs) => compileBody(rhs, potentialOutput)
      case Block(stats, expr) => compileBlock(stats) + compileBody(expr, potentialOutput)

      case app@Apply(fun: Select, args: List[Tree]) => {
        if (potentialOutput) {
          if ("apply".equals(fun.name.toString)) {
            //complex type output
            val stmtBlock = args.map(arg => {
              val outputIde = nextOutputIdentifier()
              outputs += new CoGaUDFOutput(outputIde, arg.tpe)
              toCode(CoGaExpr.Assignment, toCode(CoGaExpr.CoGaOutputCol, outputIde), compileExpr(arg))
            })
            stmtBlock.mkString
          } else {
            //single output
            val outputIde = nextOutputIdentifier()
            outputs += new CoGaUDFOutput(outputIde, app.tpe)
            toCode(CoGaExpr.Assignment, toCode(CoGaExpr.CoGaOutputCol, outputIde), compileExpr(app))
          }
        } else {
          if ("apply".equals(fun.name.toString)) {
            //complex type init
            val stmtBlock = args.map(arg => toCode(CoGaExpr.NoOp, compileExpr(app)))
            stmtBlock.mkString
          } else {
            //single stmt
            toCode(CoGaExpr.NoOp, compileExpr(app))
          }
        }
      }

      case app@Apply(fun: TypeApply, args: List[Tree]) => {
        //for Tuple initialization
        if (potentialOutput) {
          val stmtBlock = args.map(arg => {
            val outputIde = nextOutputIdentifier()
            outputs += new CoGaUDFOutput(outputIde, arg.tpe)
            toCode(CoGaExpr.Assignment, toCode(CoGaExpr.CoGaOutputCol, outputIde), compileExpr(arg))
          })
          stmtBlock.mkString
        } else {
          val results = args.map(arg => compileExpr(arg))
          toCode(CoGaExpr.NoOp, compileExpr(app))
        }
      }

      case Assign(lhs: Ident, rhs) =>
        toCode(CoGaExpr.Assignment, generateVar(lhs), compileExpr(rhs))

      case ValDef(mods, name, tpt: TypeTree, rhs) =>
        toCode(CoGaExpr.Assignment, generateVarDef(name, tpt), compileExpr(rhs))

      case If(cond, thenp, elsep) => elsep match {
        case Literal(_) => toCode(CoGaExpr.IfThen, compileExpr(cond), compileBody(thenp, false))
        case _ => toCode(CoGaExpr.IfThenElse, compileExpr(cond), compileBody(thenp, false), compileBody(elsep, false))
      }

      case ide@Ident(name: Name) => {
        if (potentialOutput) {
          //if single output, otherwise complex
          if (Type.isScalaBasicType(ide.tpe.toString)) {
            val outputIde = nextOutputIdentifier()
            outputs += new CoGaUDFOutput(outputIde, ide.tpe)
            //if input parameter, otherwise local variable
            if (closure.symbolTable isDefinedAt name.toString) {
              val coGaColumn = closure.symbolTable(name.toString)
              toCode(CoGaExpr.Assignment, toCode(CoGaExpr.CoGaOutputCol, outputIde), toCode(CoGaExpr.CoGaInputCol,
                coGaColumn))
            } else {
              toCode(CoGaExpr.Assignment, toCode(CoGaExpr.CoGaOutputCol, outputIde), name.toString)
            }
          } else {
            //currently assume that only input parameter can be complex (UDT)
            //i.e. instantiation of complex type not allowed except as final statement
            val stmtBlock = ide.tpe.members.filter(!_.isMethod).toList.reverse
                            .map { fieldIdentifier =>
                              val outputIde = nextOutputIdentifier()
                              outputs += new CoGaUDFOutput(outputIde, fieldIdentifier.typeSignature)
                              val coGaColumn = closure.symbolTable(name.toString + "." +
                                fieldIdentifier.name.toString.trim)
                              toCode(CoGaExpr.Assignment, toCode(CoGaExpr.CoGaOutputCol, outputIde),
                                toCode(CoGaExpr.CoGaInputCol, coGaColumn))
                            }
            stmtBlock.mkString
          }
        } else {
          throw new IllegalArgumentException(s"Ident for non output not implemented yet: ${showRaw(tree)}")
        }
      }

      case lit@Literal(_) => {
        val outputIde = nextOutputIdentifier()
        outputs += new CoGaUDFOutput(outputIde, lit.tpe)
        toCode(CoGaExpr.Assignment, toCode(CoGaExpr.CoGaOutputCol, outputIde), generateConst(lit))
      }

      case _ => throw new IllegalArgumentException(s"Generation of Stmt for tree type not supported: ${showRaw(tree)}")
    }
  }

  private def compileBlock(trees: List[Tree])
    (implicit closure: UDFClosure, outputs: ListBuffer[CoGaUDFOutput]): String = {
    if (trees.isEmpty) {
      ""
    } else {
      compileBody(trees.head, false) + compileBlock(trees.tail)
    }
  }

  private def compileExpr(tree: Tree)(implicit closure: UDFClosure, outputs: ListBuffer[CoGaUDFOutput]): String = {
    println("----------------- Raw tree ---------------------")
    println(showRaw(tree))

    tree match {
      case Apply(fun: Select, args: List[Tree]) => compileApply(fun, args)
      case sel@Select(qualifier, name) => compileSelect(sel)
      case ide@Ident(_) => generateVar(ide)
      case lit@Literal(_) => generateConst(lit)
      case _ => throw new IllegalArgumentException(s"Generation of Expr for tree type not supported: ${showRaw(tree)}")
    }
  }

  //  private def generateStmt(asgn: Assign, context: Map[String, (String, Type)]): Assignment = asgn match {
  //    case Assign(lhs: Ident, rhs) => Assignment(AOperator.Eq, generateVar(lhs), generateExpr(rhs, context))
  //    case _ => throw new IllegalArgumentException("Assign type not supported for: " + showRaw(asgn))
  //  }

  private def compileApply(fun: Select, args: List[Tree])
    (implicit closure: UDFClosure, outputs: ListBuffer[CoGaUDFOutput]): String = {
    if (fun.name.toString.startsWith("$")) {
      args.size match {
        case 1 => toCode(CoGaExpr.BinaryOperation, BOperator.fromScalaOp(fun, fun.tpe.finalResultType.toString),
          compileExpr(fun.qualifier), compileExpr(args.head))
        case _ => throw new IllegalArgumentException(s"Apply with args size not supported: ${args.size}")
      }
    } else {
      args.size match {
        case 1 => toCode(CoGaExpr.UnaryOperation, UOperator.fromScalaOp(fun), compileExpr(args(0)))
        case 2 => toCode(CoGaExpr.BinaryOperation, BOperator.fromScalaOp(fun, fun.tpe.finalResultType.toString),
          compileExpr(args(0)), compileExpr(args(1)))
        case _ => throw new IllegalArgumentException(s"Apply with args size ${args.size} for function${
          fun.name.toString
        } not supported.")
      }
    }
  }

  private def isCast(name: Name): Boolean = name.toString match {
    case "toDouble" | "toFloat" | "toInt" | "toLong" => true
    case _ => false
  }

  private def compileSelect(sel: Select)(implicit closure: UDFClosure, outputs: ListBuffer[CoGaUDFOutput]): String = {
    if (isCast(sel.name)) {
      sel.qualifier match {
        case ide@Ident(_) => toCode(CoGaExpr.UnaryOperation, UOperator.fromScalaOp(sel), compileExpr(ide))
        case _ => throw new IllegalArgumentException(s"Select with qualifier ${showRaw(sel.qualifier)} and name ${
          sel.name.toString
        } not supported")
      }
    } else {
      //      val tableName = qualifier match {
      //        case sel@Select(_, _) => createTableName(qualifier)
      //        case Ident(name) => context(name.toString)._1
      //        case _ => throw new IllegalArgumentException(
      //          s"Select with qualifier ${showRaw(qualifier)} and name ${name.toString} not supported")
      //      }
      if (closure.symbolTable isDefinedAt sel.toString) {
        val coGaColumn = closure.symbolTable(sel.toString)
        toCode(CoGaExpr.CoGaInputCol, coGaColumn)
      } else {
        throw new IllegalArgumentException(s"No input mapping found for ${sel.toString()}")
      }
    }
  }

  private def generateVar(ide: Ident): String =
    toCode(CoGaExpr.LocalVar, ide.name.toString)

  private def generateVarDef(name: Name, tpt: TypeTree): String =
    toCode(CoGaExpr.VarDef, Type.fromScalaType(TypeExtractor.extractNameFrom(tpt)).toString, name.toString)

  private def generateConst(lit: Literal): String =
    toCode(CoGaExpr.Const, lit.value.tpe.toString, lit.value.value.toString)


  sealed abstract class ScalaUDF(ast: Tree)

  case class ScalaMapUDF(ast: Tree) extends ScalaUDF(ast)

  case class ScalaFilterUDF(ast: Tree) extends ScalaUDF(ast)

  class UDFClosure {
    val symbolTable = mutable.Map.empty[String, String]
  }

  case class CoGaUDF(udf: String, output: Seq[CoGaUDFOutput])

}
