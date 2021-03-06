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
package ast

import scala.reflect.macros.blackbox
import scala.tools.nsc.Global

/**
 * Implements various utility functions that mitigate and/or workaround deficiencies in Scala's
 * macros APIs, e.g. non- idempotent type checking, lack of hygiene, capture-avoiding
 * substitution, fully-qualified names, fresh name generation, identifying closures, etc.
 */
trait MacroAST extends AST {

  val c: blackbox.Context
  val universe: c.universe.type = c.universe
  import universe._

  private[ast] def freshNameSuffix: Char = 'm'

  private[ast] def setOriginal(tpt: TypeTree, original: Tree): TypeTree =
    internal.setOriginal(tpt, original)

  def enclosingOwner: Symbol =
    c.internal.enclosingOwner

  def inferImplicit(tpe: Type): Option[Tree] = for {
    value <- Option(c.inferImplicitValue(tpe)) if value.nonEmpty
  } yield internal.setType(value, value.tpe.finalResultType)

  // ---------------------------
  // Parsing and type-checking
  // ---------------------------

  def warning(msg: String, pos: Position = NoPosition): Unit =
    c.warning(pos, msg)

  def abort(msg: String, pos: Position = NoPosition): Nothing =
    c.abort(pos, msg)

  def parse(code: String) =
    c.parse(code)

  def typeCheck(tree: Tree, typeMode: Boolean = false): Tree =
    if (typeMode) c.typecheck(tree, c.TYPEmode) else c.typecheck(tree)

  def eval[T](code: Tree): T =
    c.eval[T](c.Expr[T](unTypeCheck(code)))

  /** Shows `tree` in a Swing AST browser. */
  def browse(tree: Tree): Unit = universe match {
    case global: Global =>
      val gt = tree.asInstanceOf[global.Tree]
      import global.treeBrowsers._
      val frame = new BrowserFrame("macro-expand")
      val lock = new concurrent.Lock
      frame.setTreeModel(new ASTTreeModel(gt))
      frame.createFrame(lock)
      lock.acquire()
    case _ =>
  }
}
