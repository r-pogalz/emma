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

  private val basicTypeColumnIdentifier = "VALUE"

  def transform: MapUdf = {
    val mapUdfCode = transformToMapUdfCode(ast, true)
    MapUdf(outputs, mapUdfCode.map(MapUdfCode(_)))
  }

  private def freshVarName = freshTermName("map_udf_local_var_")

  private def transformToMapUdfCode(ast: Tree, isPotentialOut: Boolean = false):
  Seq[String] = {
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
          if (ifAst.tpe.isScalaBasicType) {
            val freshLocalVar = freshVarName
            val valDef = generateLineStmt(generateVarDef(ifAst.tpe.toCPrimitive, freshLocalVar))
            val transformedCond = transformToMapUdfCode(cond).mkString
            val transformedThenp = generateAssignmentStmt(s"$freshLocalVar", transformToMapUdfCode(thenp).mkString)
            val transformedElsep = generateAssignmentStmt(s"$freshLocalVar", transformToMapUdfCode(elsep).mkString)
            val finalUDFStmt = generateAssignmentStmt(generateOutputExpr(newMapUDFOutput(ifAst.tpe)), s"$freshLocalVar")
            Seq(valDef) ++
              generateIfThenElseStmt(transformedCond, transformedThenp, transformedElsep) ++
              finalUDFStmt
          } else {
            //complex type
            //currently this case should only occur for filter UDF rewrites to flatMap UDF
            val transformedCond = transformToMapUdfCode(cond).mkString
            //flatten thenp stmt
            val transformedThenp = transformToMapUdfCode(thenp, true)
            val transformedElsep = Seq(generateLineStmt("NONE"))
            generateIfThenElseStmt(transformedCond, transformedThenp, transformedElsep)
          }
        }
      }
    } else {
      generateIfThenElseStmt(transformToMapUdfCode(ifAst.cond).mkString, transformToMapUdfCode(ifAst.thenp),
        transformToMapUdfCode(ifAst.elsep))
    }
  }

  private def transformLiteral(lit: Literal, isFinalStmt: Boolean, infix: String = ""): Seq[String] = {
    if (isFinalStmt) {
      generateAssignmentStmt(generateOutputExpr(newMapUDFOutput(lit.value.tpe, infix)), Const(lit.value))
    } else {
      Seq(Const(lit.value))
    }
  }

  private def transformIdent(ide: Ident, isFinalStmt: Boolean): Seq[String] = {
    if (isFinalStmt) {
      val tblName = symbolTable(ide.name.toString)
      if (ide.tpe.isScalaBasicType) {
        //if input parameter, otherwise local variable
        if (symbolTable isDefinedAt ide.name.toString) {
          generateAssignmentStmt(generateOutputExpr(newMapUDFOutput(ide.tpe)),
            generateColAccess(tblName, basicTypeColumnIdentifier))
        } else {
          generateAssignmentStmt(generateOutputExpr(newMapUDFOutput(ide.tpe)), ide.name.toString)
        }
      } else{
        //flatten complex input param (UDT)
        transformAndFlattenComplexInputOutput(tblName, ide.tpe)
      }
    } else {
      Seq(generateLocalVar(ide.name))
    }
  }

  private def mapFieldNameToType(members: List[Symbol], types: List[Type]): List[(String, Type)] = {
    if (members.isEmpty) List()
    else (members.head.name.toString.trim, types.head) +: mapFieldNameToType(members.tail, types.tail)
  }

  private def transformAndFlattenComplexInputOutput(tblName: String, tpe: Type, infix: String = ""): Seq[String] = {
    val valueMembers = tpe.members.filter(!_.isMethod).toList.reverse
    var fieldNamesAndTypes = valueMembers.map(v => (v.name.toString.trim,v.typeSignature))
    val typeArgs = tpe.typeArgs
    if(!typeArgs.isEmpty) {
      //tpe is a Tuple
      fieldNamesAndTypes = mapFieldNameToType(valueMembers, typeArgs)
    }

    fieldNamesAndTypes.flatMap { field => {
      val currFieldName = field._1
      val currTpe = field._2
      if (currTpe.isScalaBasicType) {
        generateAssignmentStmt(generateOutputExpr(newMapUDFOutput(currTpe, s"$infix${currFieldName}_")),
          generateColAccess(tblName, s"$infix${currFieldName}"))
      } else {
        transformAndFlattenComplexInputOutput(tblName, currTpe, s"$infix${currFieldName}_")
      }
    }
    }
  }

  private def transformTypeApply(typeApply: TypeApply, args: List[Tree], isFinalSmt: Boolean): Seq[String] = {
    //initialization of a Tuple type
    if (isFinalSmt) {
      transformComplexOutputInitialization(typeApply, true, args)
    } else {
      throw new IllegalArgumentException(s"Instantiation of ${typeApply.toString()} not allowed at this place.")
    }
  }

  //generate code for complex output initialization and flatten if necessary
  private def transformComplexOutputInitialization(app: Tree, isComplex: Boolean, args: List[Tree],
    infix: String = ""): Seq[String] = {
    if (isComplex) {
      var mapped = mapFieldNameToArg(extractValueMembers(app.tpe), args)
      mapped.flatMap(tuple =>
        tuple._2 match {
          case lit: Literal => transformLiteral(lit, true, s"$infix${tuple._1}_")
          case appp@Apply(sell: Select, argss: List[Tree]) =>
            transformComplexOutputInitialization(appp, isInstantiation(sell.name), argss, s"$infix${tuple._1}_")
          case Apply(typeApply: TypeApply, args: List[Tree]) =>
            transformComplexOutputInitialization(typeApply, true, args, s"$infix${tuple._1}_")
          case _ =>
            generateAssignmentStmt(generateOutputExpr(newMapUDFOutput(tuple._2.tpe, s"$infix${tuple._1}_")),
              transformToMapUdfCode(tuple._2).mkString)
        })
    } else {
      generateAssignmentStmt(generateOutputExpr(newMapUDFOutput(app.tpe, infix)), transformToMapUdfCode(app).mkString)
    }
  }

  private def extractValueMembers(tpe: Type): List[Symbol] =
    if (!tpe.members.isEmpty) tpe.members.filter(!_.isMethod).toList.reverse
    else tpe.paramLists.head

  private def mapFieldNameToArg(members: List[Symbol], args: List[Tree]): Seq[(String, Tree)] = {
    if (members.isEmpty) Seq()
    else (members.head.name.toString.trim, args.head) +: mapFieldNameToArg(members.tail, args.tail)
  }

  private def transformSelectApply(app: Apply, sel: Select, args: List[Tree], isFinalSmt: Boolean): Seq[String] = {
    if (isFinalSmt) {
      //initialization of a complex type
      transformComplexOutputInitialization(app, isInstantiation(sel.name), args)
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
    val split = sel.toString.split("\\.")
    if (sel.symbol.toString.startsWith("method") && isSupportedUnaryMethod(sel.name)) {
      Seq(generateUnaryOp(sel.name.toTermName, transformToMapUdfCode(sel.qualifier).mkString))
    } else if (symbolTable isDefinedAt (split.head)) {
      Seq(generateColAccess(symbolTable(split.head), split.tail.mkString("_")))
    } else if (sel.symbol.toString.startsWith("method") && !isSupportedUnaryMethod(sel.name)) {
      throw new IllegalArgumentException(s"Method ${sel.name} not supported.")
    } else {
      throw new IllegalArgumentException(s"Missing table mapping for parameter ${split.head}.")
    }
  }

  private def newMapUDFOutput(tpe: Type, infix: String = ""): TypeName = {
    val freshOutputName = newUDFOutput(tpe, s"MAP_UDF_RES_$infix")
    outputs += MapUdfOutAttr(s"${tpe.toJsonAttributeType}", s"$freshOutputName", s"$freshOutputName")
    freshOutputName
  }
}
