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

/** CogaDB AST algebra. */
trait Algebra[A] {

  // -------------------------------------------------------------------------
  // Predicates
  // -------------------------------------------------------------------------

  // TODO ...

  // -------------------------------------------------------------------------
  // Predicates
  // -------------------------------------------------------------------------

  //@formatter:off
  def And(conj: Seq[A]): A
  def Or(disj: Seq[A]): A
  def ColCol(lhs: A, rhs: A, cmp: A): A
  def ColConst(attr: A, const: A, cmp: A): A
  //@formatter:on

  // -------------------------------------------------------------------------
  // Leafs
  // -------------------------------------------------------------------------

  def AttrRef(table: String, col: String, result: String, version: Short): A

  def IntConst(value: Int): A

  // -------------------------------------------------------------------------
  // Comparators
  // -------------------------------------------------------------------------

  //@formatter:off
  def Equal: A
  def Unequal: A
  def GreaterThan: A
  def GreaterEqual: A
  def LessThan: A
  def LessEqual: A
  //@formatter:on
}
