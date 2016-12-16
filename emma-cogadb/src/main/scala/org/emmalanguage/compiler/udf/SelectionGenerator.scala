package org.emmalanguage.compiler.udf

import org.emmalanguage.compiler.lang.cogadb.ast._
import org.emmalanguage.compiler.udf.common._

import scala.reflect.runtime.universe._

class SelectionGenerator(ast: Tree, symbolTable: Map[String, String]) extends JsonIRGenerator[Selection]
  with TypeHelper {

  private val basicTypeColumnName = "VALUE"

  private val mappedSelectionComparators: Map[String, Comparator] = Map(
    "$eq$eq" -> Equal, //==
    "$bang$eq" -> Unequal, //!=
    "$less" -> LessThan, //<
    "$less$eq" -> LessEqual, //<=
    "$greater" -> GreaterThan, //>
    "$greater$eq" -> GreaterEqual //>=
  )

  def generate: Selection = Selection(generatePredicate(ast))

  private def generatePredicate(t: Tree): Predicate = {
    t match {
      case Function(_, body) =>
        generatePredicate(body)

      case Apply(sel: Select, args: List[Tree]) =>
        generateFor(sel, args)

      case _ => throw new IllegalArgumentException(s"Selection generation for tree type not supported: ${showRaw(t)}")
    }
  }

  private def generatePredicate(trees: List[Tree]): List[Predicate] = {
    if (trees.isEmpty) List() else generatePredicate(trees.head) :: generatePredicate(trees.tail)
  }

  private def generateFor(sel: Select, args: List[Tree]): Predicate = sel.name match {
    case TermName("$amp$amp") => And(generatePredicate(sel.qualifier) :: generatePredicate(args))
    case TermName("$bar$bar") => Or(generatePredicate(sel.qualifier) :: generatePredicate(args))
    case TermName(name) => generateAtomicPredicate(getComparator(name), sel.qualifier, args.head)
    case _ => throw new IllegalArgumentException(s"Filter operation for op ${sel.name} not supported.")
  }

  private def generateAtomicPredicate(comp: Comparator, lhs: Tree, rhs: Tree): Predicate = lhs match {
    //lhs is column
    case lhsQualifier: Select => {
      val lhsSplit = lhsQualifier.toString().split("\\.")
      val lhsTbl = symbolTable(lhsSplit.head)
      val lhsCol = lhsSplit.tail.mkString("_").toUpperCase

      createColRhsPredicate(comp, lhsTbl, lhsCol, rhs)
    }
    case Ident(name) => {
      val lhsTbl = symbolTable(s"$name")
      val lhsCol = basicTypeColumnName

      createColRhsPredicate(comp, lhsTbl, lhsCol, rhs)
    }
    //lhs is const
    case Literal(c: Constant) => rhs match {
      //lhs is const and rhs must be column
      case rhsQualifier: Select => {
        val rhsSplit = rhsQualifier.toString().split("\\.")
        val rhsTtbl = symbolTable(rhsSplit.head)
        val rhsCol = rhsSplit.tail.mkString("_").toUpperCase

        ColConst(AttrRef(rhsTtbl, rhsCol, rhsCol), matchConst(c), comp)
      }
      case Ident(name) => {
        val rhsTtbl = symbolTable(s"$name")
        val rhsCol = basicTypeColumnName

        ColConst(AttrRef(rhsTtbl, rhsCol, rhsCol), matchConst(c), comp)
      }
      case _ => throw new IllegalArgumentException(s"Const-const predicate not allowed.")
    }
  }

  private def createColRhsPredicate(comp: Comparator, lhsTbl: String, lhsCol: String, rhs: Tree) = rhs match {
    //lhs and rhs are columns
    case rhsQualifier: Select => {
      val rhsSplit = rhsQualifier.toString().split("\\.")
      val rhsTtbl = symbolTable(rhsSplit.head)
      val rhsCol = rhsSplit.tail.mkString("_").toUpperCase

      ColCol(AttrRef(lhsTbl, lhsCol, lhsCol), AttrRef(rhsTtbl, rhsCol, rhsCol), comp)
    }
    case Ident(name) => {
      val rhsTtbl = symbolTable(s"$name")
      val rhsCol = basicTypeColumnName

      ColCol(AttrRef(lhsTbl, lhsCol, lhsCol), AttrRef(rhsTtbl, rhsCol, rhsCol), comp)
    }
    //lhs is column and rhs is const
    case Literal(c: Constant) => ColConst(AttrRef(lhsTbl, lhsCol, lhsCol), matchConst(c), comp)
  }

  private def getComparator(op: String): Comparator = mappedSelectionComparators get op match {
    case Some(comp) => comp
    case None => throw new IllegalArgumentException(s"Selection operator for op $op not supported.")
  }

}
