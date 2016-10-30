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
/*
object StringSerializer extends Algebra[String] {

  // -------------------------------------------------------------------------
  // Predicates
  // -------------------------------------------------------------------------

  override def And(conj: Seq[String]): String =
    conj.mkString(" AND ")

  override def Or(disj: Seq[String]): String =
    disj.mkString(" OR ")

  override def ColCol(lhs: String, rhs: String, cmp: String): String =
    s"$lhs $cmp $rhs"

  override def ColConst(attr: String, const: String, cmp: String): String =
    s"$attr $cmp $const"

  // -------------------------------------------------------------------------
  // Leafs
  // -------------------------------------------------------------------------

  override def AttrRef(table: String, col: String, result: String, version: Short): String =
    s"$table.$col"

  def IntConst(value: Int): String =
    value.toString

  // -------------------------------------------------------------------------
  // Comparators
  // -------------------------------------------------------------------------

  override def Equal: String =
    "=="

  override def Unequal: String =
    "!="

  override def GreaterThan: String =
    ">"

  override def GreaterEqual: String =
    ">="

  override def LessThan: String =
    "<"

  override def LessEqual: String =
    "<="
}
*/