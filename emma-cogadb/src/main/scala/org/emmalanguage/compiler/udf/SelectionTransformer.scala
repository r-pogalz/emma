package org.emmalanguage.compiler.udf

import scala.collection.mutable
import scala.reflect.runtime.universe._
import internal.reificationSupport._
import org.emmalanguage.compiler.lang.cogadb.ast._
import org.emmalanguage.compiler.udf.common._

class SelectionTransformer(ast: Tree, symbolTable: Map[String, String]) extends UDFTransformer[Selection]
  with TypeHelper {

  private val mappedSelectionComparators: Map[String, Comparator] = Map(
    "$eq$eq" -> Equal, //==
    "$less" -> LessThan, //<
    "$less$eq" -> LessEqual, //<=
    "$greater" -> GreaterThan, //>
    "$greater$eq" -> GreaterEqual //>=
  )

  def transform: Selection = Selection(generatePredicate(ast))

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
    case TermName(name) => generateAtomicPredicate(name, sel.qualifier, args.head)
    case _ => throw new IllegalArgumentException(s"Filter operation for op ${sel.name} not supported.")
  }

  def generateAtomicPredicate(op: String, lhs: Tree, rhs: Tree): Predicate = lhs match {
    //lhs is column
    case lhsQualifier: Select => {
      val lhsSplit = lhsQualifier.toString().split("\\.")
      val lhsTtbl = symbolTable(lhsSplit.head)
      val lhsCol = lhsSplit.tail.mkString("_")

      rhs match {
        //lhs and rhs are columns
        case rhsQualifier: Select => {
          val rhsSplit = rhsQualifier.toString().split("\\.")
          val rhsTtbl = symbolTable(rhsSplit.head)
          val rhsCol = rhsSplit.tail.mkString("_")

          val comparator = getComparator(op)
          ColCol(AttrRef(lhsTtbl, lhsCol, lhsCol), AttrRef(rhsTtbl, rhsCol, rhsCol), comparator)
        }
        //lhs is column and rhs is const
        case Literal(c: Constant) => ColConst(AttrRef(lhsTtbl, lhsCol, lhsCol), matchConst(c), getComparator(op))
      }
    }
    //lhs is const
    case Literal(c: Constant) => rhs match {
      //lhs is const and rhs must be column
      case rhsQualifier: Select => {
        val rhsSplit = rhsQualifier.toString().split("\\.")
        val rhsTtbl = symbolTable(rhsSplit.head)
        val rhsCol = rhsSplit.tail.mkString("_")

        val comparator = getComparator(op)
        ColConst(AttrRef(rhsTtbl, rhsCol, rhsCol), matchConst(c), comparator)
      }
      case _ => throw new IllegalArgumentException(s"Const-const predicate not allowed.")
    }
  }

  private def getComparator(op: String): Comparator = mappedSelectionComparators get op match {
    case Some(comp) => comp
    case None => throw new IllegalArgumentException(s"Selection operator for op $op not supported.")
  }

  private def matchConst(c: Constant): Const =
    if (c.tpe == typeOf[Short] || c.tpe == typeOf[Int])
      IntConst(c.value.asInstanceOf[Int])
    else if (c.tpe == typeOf[Float])
           FloatConst(c.value.asInstanceOf[Float])
    else if (c.tpe == typeOf[Double])
           DoubleConst(c.value.asInstanceOf[Double])
    else if (c.tpe == typeOf[String] || c.tpe == typeOf[java.lang.String])
           VarCharConst(c.value.asInstanceOf[String])
    else if (c.tpe == typeOf[Char])
           CharConst(c.value.asInstanceOf[Char])
    else if (c.tpe == typeOf[Boolean])
           BoolConst(c.value.asInstanceOf[String])
    else
      throw new IllegalArgumentException(s"Constant $c of type ${c.tpe} not supported.")

}
