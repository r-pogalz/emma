package org.emmalanguage
package compiler.udf


import scala.collection.mutable
import scala.reflect.runtime.universe._
import internal.reificationSupport._
import org.emmalanguage.compiler.lang.cogadb.ast._
import org.emmalanguage.compiler.udf.common._

class MapUDFTransformer(ast: Tree, symbolTable: Map[String, String]) extends UDFTransformer[MapUdf] with AnnotatedC
  with TypeHelper {

  private val outputs = mutable.ListBuffer.empty[MapUdfOutAttr]
  
  def transform: MapUdf = {
    val mapUdfCode = transformToMapUdfCode(ast, true)
    MapUdf(outputs, mapUdfCode.map(MapUdfCode(_)))
  }

  private def freshVarName = freshTermName("map_udf_local_var_")

  private def transformToMapUdfCode(ast: Tree, isPotentialOut: Boolean = false) : Seq[String] = {
    ast match {
      case Function(_, body) =>
        transformToMapUdfCode(body, isPotentialOut)

//      case DefDef(_, _, _, _, _, rhs) =>
//        transformToMapUdfCode(rhs, isPotentialOut)

      //TODO: check if this case is really necessary
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

  private def transformIfThenElse(ifAst: If, isPotentialOut: Boolean): Seq[String] = {
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

  private def transformLiteral(lit: Literal, isFinalStmt: Boolean): Seq[String] = {
    if (isFinalStmt) {
      generateAssignmentStmt(generateOutputExpr(newMapUDFOutput(lit.tpe)), Const(lit.value))
    } else {
      Seq(Const(lit.value))
    }
  }

  private def transformIdent(ide: Ident, isFinalStmt: Boolean): Seq[String] = {
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

  private def transformTypeApply(typeApply: TypeApply, args: List[Tree], isFinalSmt: Boolean): Seq[String] = {
    //initialization of a Tuple type
    if (isFinalSmt) {
      args.flatMap(arg => {
        generateAssignmentStmt(generateOutputExpr(newMapUDFOutput(arg.tpe)), transformToMapUdfCode(arg).mkString)
      })
    } else {
      throw new IllegalArgumentException(s"Instantiation of ${typeApply.toString()} not allowed at this place.")
    }
  }

  private def transformSelectApply(app: Apply, sel: Select, args: List[Tree], isFinalSmt: Boolean): Seq[String] = {
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

  private def transformSelect(sel: Select, isFinalStmt: Boolean): Seq[String] = {
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

  private def newMapUDFOutput(tpe: Type): TypeName = {
    val freshOutputName = newUDFOutput(tpe, "MAP_UDF_RES_")
    outputs += MapUdfOutAttr(s"${tpe.toJsonAttributeType}", s"$freshOutputName", s"$freshOutputName")
    freshOutputName
  }
}
