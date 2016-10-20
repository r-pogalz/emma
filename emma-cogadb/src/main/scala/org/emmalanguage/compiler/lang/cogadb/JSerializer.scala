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

import net.liftweb.json._

import scala.language.implicitConversions

object JSerializer extends Algebra[JValue] {

  implicit def stringToJString(v: String): JString =
    JString(v)

  implicit def shortToJInt(v: Short): JInt =
    JInt(v.toInt)

  implicit def intToJInt(v: Int): JInt =
    JInt(v)

  // -------------------------------------------------------------------------
  // Predicates
  // -------------------------------------------------------------------------

  override def And(conj: Seq[JValue]): JValue =
    JObject(
      JField("PREDICATE_TYPE", "AND_PREDICATE"),
      JField("PREDICATES", JArray(conj.toList.map(c => JObject(
        JField("PREDICATE", c)
      ))))
    )

  override def Or(disj: Seq[JValue]): JValue =
    JObject(
      JField("PREDICATE_TYPE", "OR_PREDICATE"),
      JField("PREDICATES", JArray(disj.toList.map(d => JObject(
        JField("PREDICATE", d)
      ))))
    )

  override def ColCol(lhs: JValue, rhs: JValue, cmp: JValue): JValue =
    JObject(
      JField("PREDICATE_TYPE", "COLUMN_COLUMN_PREDICATE"),
      JField("LEFT_HAND_SIDE_ATTRIBUTE_REFERENCE", lhs),
      JField("PREDICATE_COMPARATOR", cmp),
      JField("RIGHT_HAND_SIDE_ATTRIBUTE_REFERENCE", rhs)
    )

  override def ColConst(attr: JValue, const: JValue, cmp: JValue): JValue =
    JObject(
      JField("PREDICATE_TYPE", "COLUMN_CONSTANT_PREDICATE"),
      JField("ATTRIBUTE_REFERENCE", attr),
      JField("PREDICATE_COMPARATOR", cmp),
      JField("CONSTANT", const)
    )

  // -------------------------------------------------------------------------
  // Leafs
  // -------------------------------------------------------------------------

  override def AttrRef(table: String, col: String, result: String, version: Short): JValue =
    JObject(
      JField("TABLE_NAME", table),
      JField("COLUMN_NAME", col),
      JField("VERSION", version),
      JField("RESULT_NAME", result)
    )

  def IntConst(value: Int): JValue =
    JObject(
      JField("CONSTANT_VALUE", value),
      JField("CONSTANT_TYPE", "INT")
    )

  // -------------------------------------------------------------------------
  // Comparators
  // -------------------------------------------------------------------------

  override def Equal: JString =
    JString("EQUAL")

  override def Unequal: JValue =
    JString("UNEQUAL")

  override def GreaterThan: JValue =
    JString("GREATER_THAN")

  override def GreaterEqual: JValue =
    JString("GREATER_EQUAL")

  override def LessThan: JValue =
    JString("LESS_THAN")

  override def LessEqual: JValue =
    JString("LESS_EQUAL")
}
