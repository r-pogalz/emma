package eu.stratosphere.emma.macros.utility

import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context

class UtilMacros(val c: Context) {

  import c.universe._

  def desugar(e: c.Expr[Any]) = {
    val s = show(e.tree)
    c.Expr(
      Literal(Constant(s))
    )
  }

  def desugarRaw(e: c.Expr[Any]) = {
    val s = showRaw(e.tree)
    c.Expr(
      Literal(Constant(s))
    )
  }
}