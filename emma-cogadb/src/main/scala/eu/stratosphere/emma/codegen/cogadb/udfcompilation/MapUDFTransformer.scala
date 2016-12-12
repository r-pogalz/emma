package eu.stratosphere.emma.codegen.cogadb.udfcompilation

import eu.stratosphere.emma.codegen.cogadb.udfcompilation.common._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._
import internal.reificationSupport._

class MapUDFTransformer extends UDFTransformer with AnnotatedC with TypeHelper {

  def transform(ast: Tree, symTbl: Map[String, String]): TransformedUDF = {
    implicit val symbolTable = symTbl
    implicit val outputs = mutable.ListBuffer.empty[UDFOutput]

    val mapUdfCode = transformToMapUdfCode(ast, true)
    TransformedUDF(mapUdfCode, outputs)
  }

  private def freshVarName = freshTermName("map_udf_local_var_")

  private def transformToMapUdfCode(ast: Tree, isPotentialOut: Boolean = false)
    (implicit symbolTable: Map[String, String], outputs: ListBuffer[UDFOutput]): Seq[String] = {
    ast match {
      case Function(_, body) =>
        transformToMapUdfCode(body, isPotentialOut)

      case DefDef(_, _, _, _, _, rhs) =>
        transformToMapUdfCode(rhs, isPotentialOut)

      case Block(Nil, expr) =>
        transformToMapUdfCode(expr, isPotentialOut)

      case Block(xs, expr) =>
        xs.flatMap(transformToMapUdfCode(_)) ++ transformToMapUdfCode(expr, isPotentialOut)

      case Apply(typeApply: TypeApply, args: List[Tree]) =>
        transformTypeApply(typeApply, args, isPotentialOut)

      case app@Apply(sel: Select, args: List[Tree]) =>
        transformSelectApply(app, sel, args, isPotentialOut)

      case sel: Select =>
        transformSelect(sel, isPotentialOut)

      case Assign(lhs: Ident, rhs) =>
        generateAssignmentStmt(generateLocalVar(lhs.name), transformToMapUdfCode(rhs).mkString)

      case ValDef(mods, name, tpt: TypeTree, rhs) =>
        generateAssignmentStmt(generateVarDef(tpt.tpe.toCPrimitive, name), transformToMapUdfCode(rhs).mkString)

      case ifAst: If =>
        transformIfThenElse(ifAst, isPotentialOut)

      case ide: Ident =>
        transformIdent(ide, isPotentialOut)

      case lit: Literal =>
        transformLiteral(lit, isPotentialOut)

      case _ => throw new IllegalArgumentException(s"Code generation for tree type not supported: ${showRaw(ast)}")
    }
  }

  def transformIfThenElse(ifAst: If, isPotentialOut: Boolean)
    (implicit symbolTable: Map[String, String], outputs: ListBuffer[UDFOutput]): Seq[String] = {
    if (isPotentialOut) {
      ifAst match {
        case If(cond, thenp: Block, elsep: Block) => {
          //both branches are block stmts
          val freshLocalVar = freshVarName
          val transformedCond = transformToMapUdfCode(cond).mkString
          val transformedThenp = thenp.stats.flatMap(transformToMapUdfCode(_)) ++
            generateAssignmentStmt(s"$freshLocalVar", transformToMapUdfCode(thenp.expr).mkString)
          val transformedElsep = elsep.stats.flatMap(transformToMapUdfCode(_)) ++
            generateAssignmentStmt(s"$freshLocalVar", transformToMapUdfCode(elsep.expr).mkString)

          Seq(generateLineStmt(generateVarDef(ifAst.tpe.toCPrimitive, freshLocalVar))) ++
            generateIfThenElseStmt(transformedCond, transformedThenp, transformedElsep) ++
            generateAssignmentStmt(generateOutputExpr(newMapUDFOutput(ifAst.tpe)), s"$freshLocalVar")
        }

        case If(cond, thenp: Block, elsep) => {
          //only thenp is block stmt
          val freshLocalVar = freshVarName
          val transformedCond = transformToMapUdfCode(cond).mkString
          val transformedThenp = thenp.stats.flatMap(transformToMapUdfCode(_)) ++
            generateAssignmentStmt(s"$freshLocalVar", transformToMapUdfCode(thenp.expr).mkString)
          val transformedElsep = generateAssignmentStmt(s"$freshLocalVar", transformToMapUdfCode(elsep).mkString)

          Seq(generateLineStmt(generateVarDef(ifAst.tpe.toCPrimitive, freshLocalVar))) ++
            generateIfThenElseStmt(transformedCond, transformedThenp, transformedElsep) ++
            generateAssignmentStmt(generateOutputExpr(newMapUDFOutput(ifAst.tpe)), s"$freshLocalVar")
        }

        case If(cond, thenp, elsep: Block) => {
          //only elsep is block stmt
          val freshLocalVar = freshVarName
          val transformedCond = transformToMapUdfCode(cond).mkString
          val transformedThenp = generateAssignmentStmt(s"$freshLocalVar", transformToMapUdfCode(thenp).mkString)
          val transformedElsep = elsep.stats.flatMap(transformToMapUdfCode(_)) ++
            generateAssignmentStmt(s"$freshLocalVar", transformToMapUdfCode(elsep.expr).mkString)

          Seq(generateLineStmt(generateVarDef(ifAst.tpe.toCPrimitive, freshLocalVar))) ++
            generateIfThenElseStmt(transformedCond, transformedThenp, transformedElsep) ++
            generateAssignmentStmt(generateOutputExpr(newMapUDFOutput(ifAst.tpe)), s"$freshLocalVar")
        }

        case If(cond, thenp, elsep) => {
          //single then and else stmt
          val freshLocalVar = freshVarName
          Seq(generateLineStmt(generateVarDef(ifAst.tpe.toCPrimitive, freshLocalVar))) ++
            generateIfThenElseStmt(
              transformToMapUdfCode(cond).mkString,
              generateAssignmentStmt(s"$freshLocalVar", transformToMapUdfCode(thenp).mkString),
              generateAssignmentStmt(s"$freshLocalVar", transformToMapUdfCode(elsep).mkString)) ++
            generateAssignmentStmt(generateOutputExpr(newMapUDFOutput(ifAst.tpe)), s"$freshLocalVar")
        }
      }
    } else {
      generateIfThenElseStmt(transformToMapUdfCode(ifAst.cond).mkString, transformToMapUdfCode(ifAst.thenp),
        transformToMapUdfCode(ifAst.elsep))
    }
  }

  private def transformLiteral(lit: Literal, isFinalStmt: Boolean)
    (implicit symbolTable: Map[String, String], outputs: ListBuffer[UDFOutput]): Seq[String] = {
    if (isFinalStmt) {
      generateAssignmentStmt(generateOutputExpr(newMapUDFOutput(lit.tpe)), Const(lit.value))
    } else {
      Seq(Const(lit.value))
    }
  }

  private def transformIdent(ide: Ident, isFinalStmt: Boolean)
    (implicit symbolTable: Map[String, String], outputs: ListBuffer[UDFOutput]): Seq[String] = {
    if (isFinalStmt) {
      if (ide.tpe.isScalaBasicType) {
        //if input parameter, otherwise local variable
        if (symbolTable isDefinedAt ide.name.toString) {
          val tableCol = symbolTable(ide.name.toString)
          generateAssignmentStmt(generateOutputExpr(newMapUDFOutput(ide.tpe)), generateColAccess(tableCol))
        } else {
          generateAssignmentStmt(generateOutputExpr(newMapUDFOutput(ide.tpe)), ide.name.toString)
        }
      } else {
        //currently assume that only input parameter can be complex (UDT)
        //i.e. instantiation of complex type not allowed except as final statement
        ide.tpe.members.filter(!_.isMethod).toList.reverse
        .flatMap { fieldIdentifier =>
          val coGaColumn = symbolTable(ide.name.toString + "." +
            fieldIdentifier.name.toString.trim)
          generateAssignmentStmt(generateOutputExpr(newMapUDFOutput(ide.tpe)), generateColAccess(coGaColumn))
        }
      }
    } else {
      Seq(generateLocalVar(ide.name))
    }
  }

  private def transformTypeApply(typeApply: TypeApply, args: List[Tree], isFinalSmt: Boolean)
    (implicit symbolTable: Map[String, String], outputs: ListBuffer[UDFOutput]): Seq[String] = {
    //initialization of a Tuple type
    if (isFinalSmt) {
      args.flatMap(arg => {
        generateAssignmentStmt(generateOutputExpr(newMapUDFOutput(arg.tpe)), transformToMapUdfCode(arg).mkString)
      })
    } else {
      throw new IllegalArgumentException(s"Instantiation of ${typeApply.toString()} not allowed at this place.")
    }
  }

  private def transformSelectApply(app: Apply, sel: Select, args: List[Tree], isFinalSmt: Boolean)
    (implicit symbolTable: Map[String, String], outputs: ListBuffer[UDFOutput]): Seq[String] = {
    if (isFinalSmt) {
      if (isInstantiation(sel.name)) {
        //initialization of a complex type
        args.flatMap(arg => {
          generateAssignmentStmt(generateOutputExpr(newMapUDFOutput(arg.tpe)), transformToMapUdfCode(arg).mkString)
        })
      } else {
        generateAssignmentStmt(generateOutputExpr(newMapUDFOutput(app.tpe)), transformToMapUdfCode(app).mkString)
      }
    } else {
      if (isSupportedBinaryMethod(sel.name)) {
        Seq(generateBinaryOp(sel.name.toTermName, transformToMapUdfCode(sel.qualifier).mkString,
          transformToMapUdfCode(args.head).mkString))
      } else if (isSupportedLibrary(sel.qualifier)) {
        args.size match {
          case 1 => Seq(generateUnaryOp(sel.name.toTermName, transformToMapUdfCode(args(0)).mkString))
          case 2 => Seq(generateBinaryOp(sel.name.toTermName, transformToMapUdfCode(args(0)).mkString,
            transformToMapUdfCode(args(1)).mkString))
          case _ => throw new IllegalArgumentException(s"Select ${sel.name.toString} with ${args.size} arguments " +
            "not supported.")
        }
      } else {
        throw new IllegalArgumentException(s"Apply ${sel.name.toString} not supported.")
      }
    }
  }

  private def transformSelect(sel: Select, isFinalStmt: Boolean)
    (implicit symbolTable: Map[String, String], outputs: ListBuffer[UDFOutput]): Seq[String] = {
    //    if (isSupportedUnaryMethod(sel.name)) {
    //      val op = generateUnaryOp(sel.name.toTermName, transformToMapUdfCode(sel.qualifier))
    //      if (isFinalStmt)
    //        generateAssignmentStmt(generateOutputExpr(newUDFOutput(sel.tpe)),
    //          generateUnaryOp(sel.name.toTermName, transformToMapUdfCode(sel.qualifier)))
    //      else
    //        generateNoOp
    //    } else if (symbolTable isDefinedAt (sel.toString())) {
    //      if (isFinalStmt)
    //        generateAssignmentStmt(generateOutputExpr(newUDFOutput(sel.tpe)),
    //          generateColAccess(symbolTable(sel.toString)))
    //      else
    //        generateNoOp
    //    } else {
    //      throw new IllegalArgumentException(s"Select ${sel.toString} not supported.")
    //    }
    if (isSupportedUnaryMethod(sel.name)) {
      Seq(generateUnaryOp(sel.name.toTermName, transformToMapUdfCode(sel.qualifier).mkString))
    } else if (symbolTable isDefinedAt (sel.toString())) {
      Seq(generateColAccess(symbolTable(sel.toString)))
    } else {
      throw new IllegalArgumentException(s"Select ${sel.name.toString} not supported.")
    }
  }
}
