/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emmalanguage
package compiler.lang.cogadb

/** An abstract syntax tree for CoGaDB plans. */
object ast {

  trait Result

  // ---------------------------------------------------------------------------
  // Operators
  // ---------------------------------------------------------------------------
  sealed trait Node

  //@formatter:off
  sealed trait Op extends Node
  case class Sort(sortCols: Seq[SortCol]) extends Op
  case class GroupBy(groupCols: Seq[GroupCol], aggSpec: Seq[AggSpec]) extends Op
  case class Selection(predicate: Seq[Predicate]) extends Op
  case class TableScan(tableName: String, version: Int = 1) extends Op
  // TODO: define basic operators
  case class Projection(attRef: Seq[AttrRef]) extends Op
  case class MapUdf(mapUdfOutAttr: Seq[MapUdfOutAttr], mapUdfCode: Seq[MapUdfCode]) extends Op
  //@formatter:on

  // ---------------------------------------------------------------------------
  // Predicates
  // ---------------------------------------------------------------------------

  //@formatter:off
  sealed trait Predicate extends Node
  case class And(conj: Seq[Predicate]) extends Predicate
  case class Or(disj: Seq[Predicate]) extends Predicate
  case class ColCol(lhs: AttrRef, rhs: AttrRef, cmp: Comparator) extends Predicate
  case class ColConst(attr: AttrRef, const: Const, cmp: Comparator) extends Predicate
  //@formatter:on

  // ---------------------------------------------------------------------------
  // Leafs
  // ---------------------------------------------------------------------------

  case class AttrRef(table: String, col: String, result: String, version: Short = 1) extends Node
  //TODO
  case class MapUdfCode(code: String) extends Node
  case class MapUdfOutAttr(attType: String, attName: String, intVarName: String) extends Node
  case class AggSpec(aggFunc: String, attrRef: AttrRef) extends Node
  case class GroupCol(attrRef: AttrRef) extends Node
  case class SortCol(table: String, col: String, result: String, version: Short = 1, order: String) extends Node

  //@formatter:off
  sealed trait Const extends Node {
    type A
    val value: A
  }
  case class IntConst(value: Int) extends Const {
    override type A = Int
  }
  // TODO (FloatConst, VarcharConst, ...)
  case class FloatConst(value: Float) extends Const {
    override type A = Float
  }
  case class VarCharConst(value: String) extends Const {
    override type A = String
  }
  case class DoubleConst(value: Double) extends Const {
    override type A = Double
  }
  case class CharConst(value: Char) extends Const {
    override type A = Char
  }
  case class DateConst(value: String) extends Const {
    override type A = String
  }
  case class BoolConst(value: String) extends Const {
    override type A = String
  }
  //@formatter:on

  // ---------------------------------------------------------------------------
  // Comparators
  // ---------------------------------------------------------------------------

  //@formatter:off
  sealed trait Comparator extends Node
  case object Equal extends Comparator
  case object Unequal extends Comparator
  case object GreaterThan extends Comparator
  case object GreaterEqual extends Comparator
  case object LessThan extends Comparator
  case object LessEqual extends Comparator
  //@formatter:on
}
