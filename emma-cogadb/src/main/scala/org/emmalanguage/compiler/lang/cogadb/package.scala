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
package compiler.lang

import cogadb.ast

package object cogadb {

  /** Fold. */
  def fold[A](alg: Algebra[A])(root: ast.Node): A = root match {

    // -------------------------------------------------------------------------
    // Predicates
    // -------------------------------------------------------------------------

    // TODO ...

    // -------------------------------------------------------------------------
    // Predicates
    // -------------------------------------------------------------------------

    case ast.And(conj) => alg.And(conj.map(fold(alg)))
    case ast.Or(disj) => alg.Or(disj.map(fold(alg)))
    case ast.ColCol(lhs, rhs, cmp) => alg.ColCol(fold(alg)(lhs), fold(alg)(rhs), fold(alg)(cmp))
    case ast.ColConst(attr, const, cmp) => alg.ColConst(fold(alg)(attr), fold(alg)(const), fold(alg)(cmp))

    // -------------------------------------------------------------------------
    // Leafs
    // -------------------------------------------------------------------------

    case ast.AttrRef(table, col, result, version) => alg.AttrRef(table, col, result, version)

    case ast.IntConst(value) => alg.IntConst(value)
    // TODO ...

    // -------------------------------------------------------------------------
    // Comparators
    // -------------------------------------------------------------------------

    case ast.Equal => alg.Equal
    case ast.Unequal => alg.Unequal

    // TODO ...

    case _ => throw new RuntimeException("Missing match case in fold")
  }

}
