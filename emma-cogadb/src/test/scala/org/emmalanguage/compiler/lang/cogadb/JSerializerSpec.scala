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

import runtime.CoGaDB

import net.liftweb.json._
import org.junit.runner.RunWith
import org.scalatest.FreeSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner

import scala.language.implicitConversions

import java.io.FileNotFoundException

/** A spec for the [[CoGaDB]] runtime. */
@RunWith(classOf[JUnitRunner])
class JSerializerSpec extends FreeSpec with Matchers {

  "TPC-H Q01" - {

    // TODO: construct and issue TPC-H Query Q01 using CoGaDB AST nodes
    val tpchQ01 = ast.And(Seq(
      ast.ColCol(
        lhs = ast.AttrRef("lineitem", "order_id", "l_order_id"),
        rhs = ast.AttrRef("order", "id", "o_order_id"),
        cmp = ast.Equal
      ),
      ast.ColConst(
        attr = ast.AttrRef("order", "amount", "amount"),
        const = ast.IntConst(500),
        cmp = ast.Unequal
      )
    ))

    "json serialization" in {
      val exp = parse(readResourceFile("/tpch.Q1.json")) // TODO: paste the correct JSON in this file
      val act = fold(JSerializer)(tpchQ01)
      act shouldEqual exp
    }
  }

  private def readResourceFile(p: String): String =
    Option(getClass.getResourceAsStream(p))
      .map(scala.io.Source.fromInputStream)
      .map(_.getLines.toList.mkString("\n"))
      .getOrElse(throw new FileNotFoundException(p))
}