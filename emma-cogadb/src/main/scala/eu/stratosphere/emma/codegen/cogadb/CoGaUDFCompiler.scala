package eu.stratosphere.emma.codegen.cogadb

import eu.stratosphere.emma.codegen.cogadb.CoGaExpr.toCode
import eu.stratosphere.emma.codegen.helper.{OutputGenerator, TypeExtractor}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._

object CoGaUDFCompiler {

  class UDFClosure {
    val inputMapping = mutable.Map.empty[String, String]
  }
  
  case class CoGaUDF(udf: String, output: List[CoGaUDFOutput])
  
  case class CoGaUDFOutput(identifier: String, tpe: Type)

  def compile(tree: Tree)(implicit closure: UDFClosure): CoGaUDF = {
    implicit val outputs = mutable.ListBuffer.empty[CoGaUDFOutput]
    val compiledUDF = parseEntry(tree)
    new CoGaUDF(compiledUDF, outputs.toList)
  }
  
  def extractInputParams(tree: Tree): List[ValDef] = {
    tree match {
      case fun@Function(_, body: Function) => {
        body match {
          case Function(vparams, _) => {
            vparams
          }
          case _ => throw new IllegalArgumentException(s"No UDF found: ${showRaw(tree)}")
        }
      }
      case _ => throw new IllegalArgumentException(s"Entry tree type not supported: ${showRaw(tree)}")
    }
  }

  private def parseEntry(tree: Tree) (implicit closure: UDFClosure, outputs: ListBuffer[CoGaUDFOutput]): String = {
    println("----------------- Raw tree ---------------------")
    println(showRaw(tree))

    tree match {
      case fun@Function(_, body: Function) => {
        body match {
          case Function(vparams, body) => {
//            val context = InputTypeMapper.map(vparams)
            parse(body, true)
          }
          case _ => throw new IllegalArgumentException(s"No UDF found: ${showRaw(tree)}")
        }
      }
      case _ => throw new IllegalArgumentException(s"Entry tree type not supported: ${showRaw(tree)}")
    }
  }

  private def parse(tree: Tree, potentialOutput: Boolean)
                   (implicit closure: UDFClosure, outputs: ListBuffer[CoGaUDFOutput]): String = {
    tree match {
      case Block(stats, expr) => parse(stats) + parse(expr, true)
      case app@Apply(fun: Select, args: List[Tree]) => {
        if (potentialOutput) {
          if ("apply".equals(fun.name.toString)) {
            //complex type output
            val stmtBlock = args.map(arg => {
              val outputIde = OutputGenerator.nextOutputIdentifier
              outputs += new CoGaUDFOutput(outputIde, arg.tpe)
              toCode(CoGaExpr.Assignment, toCode(CoGaExpr.CoGaOutputCol, outputIde), parseExpr(arg))
            })
            stmtBlock.mkString
          } else {
            //single output
            val outputIde = OutputGenerator.nextOutputIdentifier
            outputs += new CoGaUDFOutput(outputIde, app.tpe)
            toCode(CoGaExpr.Assignment, toCode(CoGaExpr.CoGaOutputCol, outputIde), parseExpr(app))
          }
        } else {
          if ("apply".equals(fun.name.toString)) {
            //complex type init
            val stmtBlock = args.map(arg => toCode(CoGaExpr.NoOp, parseExpr(app)))
            stmtBlock.mkString
          } else {
            //single stmt
            toCode(CoGaExpr.NoOp, parseExpr(app))
          }
        }
      }
      case app@Apply(fun: TypeApply, args: List[Tree]) => {
        //for Tuple initialization
        if (potentialOutput) {
          val stmtBlock = args.map(arg => {
            val outputIde = OutputGenerator.nextOutputIdentifier
            outputs += new CoGaUDFOutput(outputIde, arg.tpe)
            toCode(CoGaExpr.Assignment, toCode(CoGaExpr.CoGaOutputCol, outputIde), parseExpr(arg))
          })
          stmtBlock.mkString
        } else {
          val results = args.map(arg => parseExpr(arg))
          toCode(CoGaExpr.NoOp, parseExpr(app))
        }
      }
      case Assign(lhs: Ident, rhs) =>
        toCode(CoGaExpr.Assignment, generateVar(lhs), parseExpr(rhs))
      case ValDef(mods, name, tpt: TypeTree, rhs) =>
        toCode(CoGaExpr.Assignment, generateVarDef(name, tpt), parseExpr(rhs))
      case If(cond, thenp, elsep) => elsep match {
        case Literal(_) => toCode(CoGaExpr.IfThen, parseExpr(cond), parse(thenp, false))
        case _ => toCode(CoGaExpr.IfThenElse, parseExpr(cond), parse(thenp, false), parse(elsep, false))
      }
      case lit@Literal(_) => {
        val outputIde = OutputGenerator.nextOutputIdentifier
        outputs += new CoGaUDFOutput(outputIde, lit.tpe)
        toCode(CoGaExpr.Assignment, toCode(CoGaExpr.CoGaOutputCol, outputIde), generateConst(lit))
      }
      case _ => throw new IllegalArgumentException(s"Generation of Stmt for tree type not supported: ${showRaw(tree)}")
    }
  }

  private def parse(trees: List[Tree])
                   (implicit closure: UDFClosure, outputs: ListBuffer[CoGaUDFOutput]): String = {
    if (trees.isEmpty) {
      ""
    } else {
      parse(trees.head, false) + parse(trees.tail)
    }
  }

  private def parseExpr(tree: Tree) (implicit closure: UDFClosure, outputs: ListBuffer[CoGaUDFOutput]): String = {
    println("----------------- Raw tree ---------------------")
    println(showRaw(tree))

    tree match {
      case Apply(fun: Select, args: List[Tree]) => parseApply(fun, args)
      case sel@Select(qualifier, name) => parseSelect(sel)
      case ide@Ident(_) => generateVar(ide)
      case lit@Literal(_) => generateConst(lit)
      case _ => throw new IllegalArgumentException(s"Generation of Expr for tree type not supported: ${showRaw(tree)}")
    }
  }

  //  private def generateStmt(asgn: Assign, context: Map[String, (String, Type)]): Assignment = asgn match {
  //    case Assign(lhs: Ident, rhs) => Assignment(AOperator.Eq, generateVar(lhs), generateExpr(rhs, context))
  //    case _ => throw new IllegalArgumentException("Assign type not supported for: " + showRaw(asgn))
  //  }

  private def parseApply(fun: Select, args: List[Tree])
                        (implicit closure: UDFClosure, outputs: ListBuffer[CoGaUDFOutput]): String = {
    if (fun.name.toString.startsWith("$")) {
      args.size match {
        case 1 => toCode(CoGaExpr.BinaryOperation, BOperator.fromScalaOp(fun, fun.tpe.finalResultType.toString),
          parseExpr(fun.qualifier), parseExpr(args.head))
        case _ => throw new IllegalArgumentException(s"Apply with args size not supported: ${args.size}")
      }
    } else {
      args.size match {
        case 1 => toCode(CoGaExpr.UnaryOperation, UOperator.fromScalaOp(fun),
          parseExpr(args(0)))
        case 2 => toCode(CoGaExpr.BinaryOperation, BOperator.fromScalaOp(fun, fun.tpe.finalResultType.toString),
          parseExpr(args(0)), parseExpr(args(1)))
        case _ => throw new IllegalArgumentException(
          s"Apply with args size ${args.size} for function ${fun.name.toString} not supported.")
      }
    }
  }

  def isCast(name: Name): Boolean = name.toString match {
    case "toDouble" | "toFloat" | "toInt" | "toLong" => true
    case _ => false
  }

  private def parseSelect(sel: Select) (implicit closure: UDFClosure, outputs: ListBuffer[CoGaUDFOutput]): String = {
    if (isCast(sel.name)) {
      sel.qualifier match {
        case ide@Ident(_) => toCode(CoGaExpr.UnaryOperation, UOperator.fromScalaOp(sel), parseExpr(ide))
        case _ => throw new IllegalArgumentException(
          s"Select with qualifier ${showRaw(sel.qualifier)} and name ${sel.name.toString} not supported")
      }
    } else {
//      val tableName = qualifier match {
//        case sel@Select(_, _) => createTableName(qualifier)
//        case Ident(name) => context(name.toString)._1
//        case _ => throw new IllegalArgumentException(
//          s"Select with qualifier ${showRaw(qualifier)} and name ${name.toString} not supported")
//      }
      if(closure.inputMapping isDefinedAt sel.toString) {
        val coGaColumn = closure.inputMapping(sel.toString)
        toCode(CoGaExpr.CoGaInputCol, coGaColumn)
      }else{
        throw new IllegalArgumentException(s"No input mapping found for ${sel.toString()}")
      }
    }
  }

  private def generateVar(ide: Ident): String = toCode(CoGaExpr.LocalVar, ide.name.toString)

  private def generateVarDef(name: Name, tpt: TypeTree): String =
    toCode(CoGaExpr.VarDef, Type.fromScalaType(TypeExtractor.extractNameFrom(tpt)).toString, name.toString)

  private def generateConst(lit: Literal): String =
    toCode(CoGaExpr.Const, lit.value.tpe.toString, lit.value.value.toString)

}
