package eu.stratosphere.emma.codegen.cogadb

import eu.stratosphere.emma.codegen.helper.{InputTypeMapper, OutputGenerator, TypeExtractor}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}
import scala.reflect.runtime.universe._

object UDFParser {

  class UDFClosure {
    val inputMapping = mutable.Map.empty[String, String]
  }

  def modifiedTree(tree: Tree) (implicit closure: UDFClosure): (Stmt, List[String])= {
    implicit val outputs = mutable.ListBuffer.empty[String]
    val compiledUDF = parseEntry(tree)
    (compiledUDF, outputs.toList)
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

  private def parseEntry(tree: Tree) (implicit closure: UDFClosure, outputs: ListBuffer[String]): Stmt = {
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
                   (implicit closure: UDFClosure, outputs: ListBuffer[String]): Stmt = {
    tree match {
      case Block(stats, expr) => StmtBlock(parse(stats) :+ parse(expr, true))
      case app@Apply(fun: Select, args: List[Tree]) => {
        if (potentialOutput) {
          if ("apply".equals(fun.name.toString)) {
            //complex type output
            val results = args.map(arg => {
              val outputIde = OutputGenerator.nextOutputIdentifier
              outputs += outputIde
              Assignment(AOperator.Eq, OutputCol(s"<OUT>.$outputIde"), parseExpr(arg))
            })
            StmtBlock(results)
          } else {
            //single output
            val outputIde = OutputGenerator.nextOutputIdentifier
            outputs += outputIde
            Assignment(AOperator.Eq, OutputCol(s"<OUT>.$outputIde"), parseExpr(app))
          }
        } else {
          if ("apply".equals(fun.name.toString)) {
            //complex type init
            val results = args.map(arg => NoOp(parseExpr(app)))
            StmtBlock(results)
          } else {
            //single stmt
            NoOp(parseExpr(app))
          }
        }
      }
      case app@Apply(fun: TypeApply, args: List[Tree]) => {
        //for Tuple initialization
        if (potentialOutput) {
          val results = args.map(arg => {
            val outputIde = OutputGenerator.nextOutputIdentifier
            outputs += outputIde
            Assignment(AOperator.Eq, OutputCol(s"<OUT>.$outputIde"), parseExpr(arg))
          })
          StmtBlock(results)
        } else {
          val results = args.map(arg => parseExpr(arg))
          NoOp(parseExpr(app))
        }
      }
      case Assign(lhs: Ident, rhs) =>
        Assignment(AOperator.Eq, generateVar(lhs), parseExpr(rhs))
      case ValDef(mods, name, tpt: TypeTree, rhs) =>
        Assignment(AOperator.Eq, generateVarDef(name, tpt), parseExpr(rhs))
      case If(cond, thenp, elsep) => elsep match {
        case Literal(_) => IfThen(parseExpr(cond), parse(thenp, false))
        case _ => IfThenElse(parseExpr(cond), parse(thenp, false), parse(elsep, false))
      }
      case lit@Literal(_) => {
        val outputIde = OutputGenerator.nextOutputIdentifier
        outputs += outputIde
        Assignment(AOperator.Eq, OutputCol(s"<OUT>.$outputIde"), generateConst(lit))
      }
      case _ => throw new IllegalArgumentException(s"Generation of Stmt for tree type not supported: ${showRaw(tree)}")
    }
  }

  private def parse(trees: List[Tree])
                   (implicit closure: UDFClosure, outputs: ListBuffer[String]): List[Stmt] = {
    if (trees.isEmpty) {
      Nil
    } else {
      parse(trees.head, false) :: parse(trees.tail)
    }
  }

  private def parseExpr(tree: Tree) (implicit closure: UDFClosure, outputs: ListBuffer[String]): Expr = {
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
                        (implicit closure: UDFClosure, outputs: ListBuffer[String]): Expr = {
    if (fun.name.toString.startsWith("$")) {
      args.size match {
        case 1 => BinaryOp(BOperator.fromScalaOp(fun, fun.tpe.finalResultType.toString),
          parseExpr(fun.qualifier), parseExpr(args.head))
        case _ => throw new IllegalArgumentException(s"Apply with args size not supported: ${args.size}")
      }
    } else {
      args.size match {
        case 1 => UnaryOp(UOperator.fromScalaOp(fun),
          parseExpr(args(0)))
        case 2 => BinaryOp(BOperator.fromScalaOp(fun, fun.tpe.finalResultType.toString),
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

  private def parseSelect(sel: Select) (implicit closure: UDFClosure, outputs: ListBuffer[String]): Expr = {
    if (isCast(sel.name)) {
      sel.qualifier match {
        case ide@Ident(_) => UnaryOp(UOperator.fromScalaOp(sel), parseExpr(ide))
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
        InputCol(coGaColumn)
      }else{
        throw new IllegalArgumentException(s"No input mapping found for ${sel.toString()}")
      }
    }
  }

  private def createTableName(qualifier: Tree): String = {
    qualifier match {
      case Select(qualifier, name) => createTableName(qualifier) + name.toString
      case Ident(name) => name.toString
    }
  }

  private def generateVar(ide: Ident): Var = LocalVar(ide.name.toString)

  private def generateVarDef(name: Name, tpt: TypeTree): Var =
    VarDef(name.toString, Type.fromScalaType(TypeExtractor.extractNameFrom(tpt)))

  private def generateConst(lit: Literal): Const = Const(lit.value.value.toString,
    Type.fromScalaType(lit.value.tpe.toString))

}
