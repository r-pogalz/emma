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

import org.emmalanguage.compiler.lang.cogadb.ast.AggSpec
import org.emmalanguage.compiler.lang.cogadb.ast.MapUdfOutAttr

import net.liftweb.json._

import scala.language.implicitConversions

object JSerializer extends Algebra[JValue] {

  implicit def stringToJString(v: String): JString =
    JString(v)

  implicit def shortToJInt(v: Short): JInt =
    JInt(v.toInt)

  implicit def intToJInt(v: Int): JInt =
    JInt(v)

  implicit def floatToJFloat(v: Float): JDouble =
    JDouble(v)

  implicit def floatToJDouble(v: Double): JDouble =
    JDouble(v)

  implicit def CharToJChar(v: Char): JString =
    JString(v.toString)


  // -------------------------------------------------------------------------
  // Operators
  // -------------------------------------------------------------------------

  override def Sort(sortCols: Seq[JValue]): JValue =
    JObject(
      JField("OPERATOR_NAME", "SORT_BY"),
      JField("SORTING_COLUMNS", JArray(sortCols.toList.map(sortCol => JObject(
        JField("", sortCol)
      ))))
    )

  override def GroupBy(groupCols: Seq[JValue],aggSpec: Seq[JValue]): JValue =
    JObject(
      JField("OPERATOR_NAME", "GENERIC_GROUPBY"),
      JField("GROUPING_COLUMNS", JArray(groupCols.toList.map(groupCol => JObject(
        JField("",groupCol)
      )))),
      JField("AGGREGATION_SPECIFICATION", JArray(aggSpec.toList.map(a => JObject(
        JField("",a)
      ))))
    )

  override def Selection(predicate: JValue): JValue =
    JObject(
      JField("OPERATOR_NAME", "GENERIC_SELECTION"),
      JField("PREDICATE", predicate)
    )

  override def TableScan(tableName: String, version: Int): JValue =
    JObject(
      JField("OPERATOR_NAME", "TABLE_SCAN"),
      JField("TABLE_NAME", tableName),
      JField("VERSION", version)
  )

  override def Projection(attrRef: Seq[JValue]): JValue =
    JObject(
      JField("OPERATOR_NAME", "PROJECTION"),
      JField("ATTRIBUTES", JArray(attrRef.toList.map(a => JObject(
        JField("",a)
      ))

      ))
    )
  override def MapUdf(mapUdfOutAttr: Seq[JValue],mapUdfCode: Seq[JValue]): JValue =
    JObject(
      JField("OPERATOR_NAME", "MAP_UDF"),
      JField("MAP_UDF_OUTPUT_ATTRIBUTES", JArray(mapUdfOutAttr.toList.map(a => JObject(
        JField("", a)
      ))))
    )


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
  //case class MapUdfCode(code: String) extends Node
  //case class MapUdfOutAttr(attType: String, attName: String, intVarName: String) extends Node
  /*case class AggSpecAggFunc: String, attrRef: AttrRef) extends Node
  case class GroupCol(attrRef: AttrRef) extends Node
  case class SortCol(table: String, col: String, result: String, version: Short = 1, order: String) extends Node*/

  override def MapUdfCode(code: String): JValue =
    JObject(
      JField("MAP_UDF_CODE", JArray(code.toList.map( c => JObject(
        JField("",c))
      )))
    )

  override def MapUdfOutAttr(attType: String, attName: String, intVarName: String): JValue =
    JObject(
      JField("ATTRIBUTE_TYPE", attType),
      JField("ATTRIBUTE_NAME", attName),
      JField("INTERNAL_VARIABLE_NAME", intVarName)
    )
  def AggSpec(aggFunc: String, attrRef: JValue): JValue =
    JObject(
      JField("AGGREGATION_SPECIFICATION", aggFunc)
    )

  def GroupCol(attrRef: JValue): JValue = ???
  def SortCol(table: String, col: String, result: String, version: Short, order: String): JValue = ???

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
  def FloatConst(value: Float): JValue =
    JObject(
      JField("CONSTANT_VALUE", value),
      JField("CONSTANT_TYPE", "FLOAT")
    )
  def VarCharConst(value: String): JValue =
    JObject(
      JField("CONSTANT_VALUE", value),
      JField("CONSTANT_TYPE", "VARCHAR")
    )
  def DoubleConst(value: Double): JValue =
    JObject(
      JField("CONSTANT_VALUE", value),
      JField("CONSTANT_TYPE", "VARCHAR")
    )
  def CharConst(value: Char): JValue =
    JObject(
      JField("CONSTANT_VALUE", value),
      JField("CONSTANT_TYPE", "CHAR")
    )
  def DateConst(value: String): JValue =
    JObject(
      JField("CONSTANT_VALUE", value),
      JField("CONSTANT_TYPE", "DATE")
    )
  def BoolConst(value: String): JValue =
    JObject(
      JField("CONSTANT_VALUE", value),
      JField("CONSTANT_TYPE", "BOOLEAN")
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
