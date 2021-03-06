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
package compiler.lang.comprehension

import compiler.Common
import compiler.lang.core.Core

import shapeless._

import scala.annotation.tailrec
import scala.collection.breakOut

private[comprehension] trait Combination extends Common {
  self: Core with Comprehension =>

  private[comprehension] object Combination {

    import Comprehension._
    import UniverseImplicits._
    import Core.{Lang => core}

    private type Rule = (u.Symbol, u.Tree) => Option[u.Tree]
    private val cs = new Comprehension.Syntax(API.bagSymbol)
    private val tuple2 = core.Ref(api.Sym.tuple(2).companion.asModule)
    private val tuple2App = tuple2.tpe.member(api.TermName.app).asMethod
    private val Seq(_1, _2) = {
      val tuple2 = api.Type[(Nothing, Nothing)]
      for (i <- 1 to 2) yield tuple2.member(api.TermName(s"_$i")).asTerm
    }

    // TODO: Split conjunctive filter predicates.

    /**
     * Introduces combinators instead of comprehensions.
     * In addition to the monad ops, it recognizes crosses and equi joins.
     *
     * == Preconditions ==
     *
     * - Input must be in ANF.
     *
     * == Postconditions ==
     *
     * - An ANF tree with no mock-comprehensions.
     */
    val transform: u.Tree => u.Tree =
      api.TopDown.withOwner.transformWith {
        case Attr.inh(comp @ cs.Comprehension(_, _), owner :: _) =>
          combine(owner, comp)
      }.andThen(_.tree)

    /**
     * Performs the combination for one comprehension. That is, the root of the given tree
     * must be a comprehension, and it eliminates only this one.
     *
     * Note: We can't just call transform with the Match... rules, because then the application
     * of the rules would be interleaved across the outer and the nested comprehensions, which
     * would mess up the order of rule applications for a single comprehension.
     */
    @tailrec def combine(owner: u.Symbol, tree: u.Tree): u.Tree =
      rules.foldLeft(Option.empty[u.Tree]) {
        (done, rule) => done orElse rule(owner, tree)
      } match {
        case Some(result) => combine(owner, result)
        case None => tree
      }

    /**
     * Creates a filter combinator.
     *
     * ==Matching Pattern==
     * {{{ [[ hd | qs1, x ← xs, qs2, p x, qs3 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     val $x = generator {
     *       $xVals
     *       $xExpr
     *     }
     *     $qs2
     *     guard {
     *       $pVals
     *       $pExpr
     *     }
     *     $qs3
     *     $hd
     *   }
     * }}}
     *
     * ==Guard==
     * - The matched guard must use at most one generator variable and this variable should be `x`.
     * - The matched generator and the matched guard should not have control flow.
     *
     * ==Rewrite==
     * {{{ [[ hd | qs1, x ← filter p xs, qs2, qs3 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     val $x = generator {
     *       $xVals
     *       $p = x => {
     *         $pVals
     *         $pExpr
     *       }
     *       val $filtered = $xExpr withFilter $p
     *       $filtered
     *     }
     *     $qs2
     *     $qs3
     *     $hd
     *   }
     * }}}
     */
    val MatchFilter: Rule = {
      case (owner, cs.Comprehension(qs, hd)) => (for {
        guard @ cs.Guard(pred) <- qs.view
        (qs12, qs3) = splitAt(guard)(qs)
        // (Seq() is to disallow control flow)
        xGen @ cs.Generator(x, core.Let(xVals, Seq(), xExpr)) <- qs12.view
        (qs1, qs2) = splitAt[u.Tree](xGen)(qs12)
        refd = api.Tree.refs(guard) intersect gens(qs12)
        if refd.isEmpty || (refd.size == 1 && refd.head == x)
      } yield {
        val (pRef, pVal) = valRefAndDef(owner, "p", core.Lambda(Seq(x), pred))
        val (fRef, fVal) = valRefAndDef(owner, "filtered", cs.WithFilter(xExpr)(pRef))
        val vals = Seq.concat(xVals, Seq(pVal, fVal))
        val gen = cs.Generator(x, core.Let(vals, Seq.empty, fRef))
        val combined = Seq.concat(qs1, Seq(gen), qs2, qs3)
        cs.Comprehension(combined, hd)
      }).headOption

      case _ => None
    }


    /**
     * Creates a flatMap combinator.
     *
     * ===Matching Pattern===
     * {{{ [[ hd | qs1, x ← xs, qs2, y ← f x, qs3 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     val $x = generator {
     *       $xVals
     *       $xExpr
     *     }
     *     $qs2
     *     val $y = generator yBody
     *     $qs3
     *     $hd
     *   }
     * }}}
     *
     * ==Guard==
     * - The generator of `y` must refer to exactly one generator variable - `x`.
     * - The remaining qualifiers, as well as the head should not refer to `x`.
     * - The matched generators should not have control flow.
     *
     * ==Rewrite==
     * {{{ [[ hd | qs1, qs2, y ← flatMap f xs, qs3 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     $qs2
     *     val $y = generator {
     *       $xVals
     *       val $f = $x => yBody
     *       val $fmapped = $xExpr flatMap $f
     *       $fmapped
     *     }
     *     $qs3
     *     $hd
     *   }
     * }}}
     */
    val MatchFlatMap: Rule = {
      case (owner, cs.Comprehension(qs, hd)) => (for {
        // (Seq() is to disallow control flow)
        xGen @ cs.Generator(x, core.Let(xVals, Seq(), xExpr)) <- qs.view
        (qs1, qs23) = splitAt[u.Tree](xGen)(qs)
        yGen @ cs.Generator(y, yRhs) <- qs23.view
        (qs2, qs3) = splitAt[u.Tree](yGen)(qs23)
        if (api.Tree.refs(yGen) intersect gens(qs)) == Set(x)
        if Seq.concat(qs2, qs3, Seq(hd)).forall(!api.Tree.refs(_).contains(x))
      } yield {
        val (fRef, fVal) = valRefAndDef(owner, "f", core.Lambda(Seq(x), yRhs))
        val (fmRef, fmVal) = valRefAndDef(owner, "fmapped", cs.FlatMap(xExpr)(fRef))
        val vals = Seq.concat(xVals, Seq(fVal, fmVal))
        val gen = cs.Generator(y, core.Let(vals, Seq.empty, fmRef))
        val combined = Seq.concat(qs1, qs2, Seq(gen), qs3)
        cs.Comprehension(combined, hd)
      }).headOption

      case _ => None
    }

    /**
     * Creates a flatMap combinator. The difference between this and the other flatMap rule, is that
     * this is able to handle the situation when x is referenced in later qualifiers or the head.
     *
     * ===Matching Pattern===
     * {{{ [[ hd | qs1, x ← xs, qs2, y ← f x, qs3 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     val $x = generator {
     *       $xVals
     *       $xExpr
     *     }
     *     $qs2
     *     val $y = generator {
     *       $yVals
     *       $yExpr
     *     }
     *     $qs3
     *     $hd
     *   }
     * }}}
     *
     * ==Guard==
     * - The generator of `y` must refer to exactly one generator variable - `x`.
     * - The matched generators should not have control flow.
     *
     * ==Rewrite==
     * {{{
     *   comprehension {
     *     $qs1
     *     val $xy = generator {
     *       $xVals
     *       val $f = $x => {
     *         $yVals
     *         val $g = $y1 => {
     *           val $ir = ($x, $y1)
     *           $ir
     *         }
     *         val $mapped = $yExpr map $g
     *         $mapped
     *       }
     *       val $fmapped = $xExpr flatMap $f
     *       $fmapped
     *     }
     *     $qs2[xy._1/x][xy._2/y]  // Note: Introduce ValDefs for xy._1 and xy._2 to maintain ANF
     *     $qs3[xy._1/x][xy._2/y]
     *     $hd[xy._1/x][xy._2/y]
     *   }
     * }}}
     */
    val MatchFlatMap2: Rule = {
      case (owner, cs.Comprehension(qs, hd)) => (for {
        // (Seq() is to disallow control flow)
        xGen @ cs.Generator(x, core.Let(xVals, Seq(), xExpr)) <- qs.view
        (qs1, qs23) = splitAt[u.Tree](xGen)(qs)
        yGen @ cs.Generator(y, core.Let(yVals, Seq(), yExpr)) <- qs23.view
        (qs2, qs3) = splitAt[u.Tree](yGen)(qs23)
        if (api.Tree.refs(yGen) intersect gens(qs)) == Set(x)
      } yield {
        val y1 = api.TermSym.free(api.TermName.fresh(y), Core.bagElemTpe(yExpr))
        val irArgs = Seq(core.Ref(x), core.Ref(y1))
        val irRhs = core.DefCall(Some(tuple2), tuple2App, Seq(x.info, y1.info), Seq(irArgs))
        val (irRef, irVal) = valRefAndDef(owner, "ir", irRhs)
        val gBody = core.Let(Seq(irVal), Seq.empty, irRef)
        val (gRef, gVal) = valRefAndDef(owner, "g", core.Lambda(Seq(y1), gBody))
        val (mRef, mVal) = valRefAndDef(owner, "mapped", cs.Map(yExpr)(gRef))
        val fBody = core.Let(yVals ++ Seq(gVal, mVal), Seq.empty, mRef)
        val (fRef, fVal) = valRefAndDef(owner, "f", core.Lambda(Seq(x), fBody))
        val (fmRef, fmVal) = valRefAndDef(owner, "fmapped", cs.FlatMap(xExpr)(fRef))
        val xyTpe = Core.bagElemTpe(fmRef)
        assert(xyTpe.typeConstructor =:= api.Sym.tuple(2).toTypeConstructor)
        val xy = api.ValSym(owner, api.TermName.fresh("xy"), xyTpe)
        val xyRef = core.Ref(xy)
        val xy1 = core.ValDef(x, core.DefCall(Some(xyRef), _1, Seq.empty, Seq.empty))
        val xy2 = core.ValDef(y, core.DefCall(Some(xyRef), _2, Seq.empty, Seq.empty))
        val bind = capture(cs, Seq(xy1, xy2)) _
        val vals = Seq.concat(xVals, Seq(fVal, fmVal))
        val gen = cs.Generator(xy, core.Let(vals, Seq.empty, fmRef))
        val combined = Seq.concat(qs1, Seq(gen), qs2.view map bind, qs3.view map bind)
        cs.Comprehension(combined, bind(hd))
      }).headOption

      case _ => None
    }

    /**
     * Creates a cross combinator.
     *
     * ==Matching Pattern==
     * {{{ [[ hd | qs1, x ← xs, y ← ys, qs3 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     val $x = generator {
     *       $xVals
     *       $xExpr
     *     }
     *     val $y = generator {
     *       $yVals
     *       $yExpr
     *     }
     *     $qs3
     *     $hd
     *   }
     * }}}
     *
     * ==Guard==
     * - The matched generators should not be correlated, i.e. the gen of `y` should not refer `x`.
     * - The matched generators should not have control flow.
     *
     * ==Rewrite==
     * {{{ [[ hd[xy._1/x][xy._2/y] | qs1, xy ← ⨯ xs ys, qs3[xy._1/x][xy._2/y] ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     val $xy = generator {
     *       $xVals
     *       $yVals
     *       val $crossed = cross($xExpr, $yExpr)
     *       $crossed
     *     }
     *     $qs3[xy._1/x][xy._2/y]  // Note: Introduce ValDefs for xy._1 and xy._2 to maintain ANF
     *     $hd[xy._1/x][xy._2/y]
     *   }
     * }}}
     */
    val MatchCross: Rule = {
      case (owner, cs.Comprehension(qs, hd)) => (for {
        // (Seq() is to disallow control flow)
        xGen @ cs.Generator(x, core.Let(xVals, Seq(), xExpr)) <- qs.view
        (qs1, qs23) = splitAt[u.Tree](xGen)(qs)
        yGen @ cs.Generator(y, core.Let(yVals, Seq(), yExpr)) <- qs23.view
        (qs2, qs3) = splitAt[u.Tree](yGen)(qs23)
        if qs2.isEmpty && !api.Tree.refs(yGen).contains(x)
      } yield {
        val (cRef, cVal) = valRefAndDef(owner, "crossed", Combinators.Cross(xExpr, yExpr))
        val xyTpe = Core.bagElemTpe(cRef)
        assert(xyTpe.typeConstructor =:= api.Sym.tuple(2).toTypeConstructor)
        val xy = api.ValSym(owner, api.TermName.fresh("xy"), xyTpe)
        val xyRef = core.Ref(xy)
        val xy1 = core.ValDef(x, core.DefCall(Some(xyRef), _1, Seq.empty, Seq.empty))
        val xy2 = core.ValDef(y, core.DefCall(Some(xyRef), _2, Seq.empty, Seq.empty))
        val bind = capture(cs, Seq(xy1, xy2)) _
        val vals = Seq.concat(xVals, yVals, Seq(cVal))
        val gen = cs.Generator(xy, core.Let(vals, Seq.empty, cRef))
        val combined = Seq.concat(qs1, Seq(gen), qs3.view map bind)
        cs.Comprehension(combined, bind(hd))
      }).headOption

      case _ => None
    }

    /**
     * Creates a join combinator.
     *
     * ==Matching Pattern==
     * {{{ [[ hd | qs1, x ← xs, y ← ys, qs3, k₁ x == k₂ y, qs4 ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     val $x = generator {
     *       $xVals
     *       $xExpr
     *     }
     *     val $y = generator {
     *       $yVals
     *       $yExpr
     *     }
     *     $qs3
     *     guard {
     *       $joinVals
     *       val $cond = $kxExpr == $kyExpr  // Or `$kyExpr == $kxExpr`
     *       $cond
     *     }
     *     $qs4
     *     $hd
     *   }
     * }}}
     *
     * ==Guard==
     * - The matched generators should not be correlated, i.e. the gen of `y` should not refer `x`.
     * - The matched guard should refer to x and y, but not to other generator variables.
     * - $kxExpr's dependencies in $joinCondVals0 should not contain y, and vice versa for $kyExpr
     * - The matched generators and guard should not have control flow.
     *
     * ==Rewrite==
     * {{{ [[ hd[xy._1/x][xy._2/y] | qs1, xy ← ⋈ k₁ k₂ xs ys, qs3[xy._1/x][xy._2/y],
     *        qs4[xy._1/x][xy._2/y] ]] }}}
     * {{{
     *   comprehension {
     *     $qs1
     *     val $xy = generator {
     *       $xVals
     *       $yVals
     *       val $kx = x => {
     *         $joinVals  // But only those vals that are needed for kxExpr
     *         $kxExpr
     *       }
     *       val $ky = y => {
     *         $joinVals  // But only those vals that are needed for kyExpr
     *         $kyExpr
     *       }
     *       val $joined = join $kx $ky $xExpr $yExpr
     *       $joined
     *     }
     *     $qs3[xy._1/x][xy._2/y]  // Note: Introduce ValDefs for j.x and j.y to maintain ANF
     *     $qs4[xy._1/x][xy._2/y]
     *     $hd[xy._1/x][xy._2/y]
     *   }
     * }}}
     */
    val MatchEquiJoin: Rule = {
      case (owner, cs.Comprehension(qs, hd)) => (for {
        // (Seq() is to disallow control flow)
        xGen @ cs.Generator(x, core.Let(xVals, Seq(), xExpr)) <- qs.view
        (qs1, qs234) = splitAt[u.Tree](xGen)(qs)
        yGen @ cs.Generator(y, core.Let(yVals, Seq(), yExpr)) <- qs234.view
        (qs2, qs34) = splitAt[u.Tree](yGen)(qs234)
        if qs2.isEmpty && !api.Tree.refs(yGen).contains(x)
        grd @ cs.Guard(core.Let(grdVals, Seq(), core.Ref(cond))) <- qs34.view
        if (api.Tree.refs(grd) intersect gens(qs)) == Set(x, y)
        condVal @ core.ValDef(`cond`, core.DefCall(Some(eqLhs), eq, _, Seq(Seq(eqRhs)))) <- grdVals
        if eq.name.toString == "$eq$eq"
        joinVals = grdVals.filter(_ != condVal)
        (kxExpr, kyExpr) <- Seq((eqLhs, eqRhs), (eqRhs, eqLhs))
        kxBody = Core.dce(core.Let(joinVals, Seq.empty, kxExpr))
        if !api.Tree.refs(kxBody).contains(y)
        kyBody = Core.dce(core.Let(joinVals, Seq.empty, kyExpr))
        if !api.Tree.refs(kyBody).contains(x)
        (qs3, qs4) = splitAt[u.Tree](grd)(qs34)
      } yield {
        // It can happen that the key functions have differing return types (e.g. Long and Int).
        // In this case, we add casts before the return expr of the key functions to the weakLub.
        def maybeCast(tree: u.Tree) = if (kxExpr.tpe =:= kyExpr.tpe) tree else tree match {
          case core.Let(valDefs, _, expr) =>
            val asInstanceOf = expr.tpe.member(api.TermName("asInstanceOf")).asTerm
            val kLub = api.Type.weakLub(Seq(kxExpr.tpe, kyExpr.tpe))
            val cast = core.DefCall(Some(expr), asInstanceOf, Seq(kLub), Seq.empty)
            val (castRef, castVal) = valRefAndDef(owner, "cast", cast)
            core.Let(valDefs :+ castVal, Seq.empty, castRef)
          case other => other
        }

        val (kxRef, kxVal) = valRefAndDef(owner, "kx", core.Lambda(Seq(x), maybeCast(kxBody)))
        val (kyRef, kyVal) = valRefAndDef(owner, "ky", core.Lambda(Seq(y), maybeCast(kyBody)))
        val join = Combinators.EquiJoin(kxRef, kyRef)(xExpr, yExpr)
        val (jRef, jVal) = valRefAndDef(owner, "joined", join)
        val xyTpe = Core.bagElemTpe(jRef)
        assert(xyTpe.typeConstructor =:= api.Sym.tuple(2).toTypeConstructor)
        val xy = api.ValSym(owner, api.TermName.fresh("xy"), xyTpe)
        val xyRef = core.Ref(xy)
        val xy1 = core.ValDef(x, core.DefCall(Some(xyRef), _1, Seq.empty, Seq.empty))
        val xy2 = core.ValDef(y, core.DefCall(Some(xyRef), _2, Seq.empty, Seq.empty))
        val bind = capture(cs, Seq(xy1, xy2)) _
        val vals = Seq.concat(xVals, yVals, Seq(kxVal, kyVal, jVal))
        val gen = cs.Generator(xy, core.Let(vals, Seq.empty, jRef))
        val combined = Seq.concat(qs1, Seq(gen), qs3.view map bind, qs4.view map bind)
        cs.Comprehension(combined, bind(hd))
      }).headOption

      case _ => None
    }

    /**
     * Eliminates the residual comprehension.
     *
     * ==Matching Pattern==
     * {{{ [[ hd | x <- xs ]] }}}
     * {{{
     *   comprehension {
     *     val $x = generator {
     *       $xVals
     *       $xExpr
     *     }
     *     $hd
     *   }
     * }}}
     *
     * ==Guard==
     * - The generator should not have control flow.
     *
     * ==Rewrite==
     * {{{ map hd xs }}}
     * {{{
     *   $xVals
     *   val $f = $fArg => $hd[fArg/x]
     *   val $mapped = $xExpr map $f
     *   $mapped
     * }}}
     */
    val MatchResidual: Rule = {
      case (own, cs.Comprehension(Seq(cs.Generator(x, core.Let(vs, Seq(), expr))), cs.Head(hd))) =>
        val (fRef, fVal) = valRefAndDef(own, "f", core.Lambda(Seq(x), hd))
        val (mRef, mVal) = valRefAndDef(own, "mapped", cs.Map(expr)(fRef))
        Some(core.Let(vs ++ Seq(fVal, mVal), Seq.empty, mRef))
      case _ => None
    }

    private val rules = Seq(
      MatchFilter, MatchFlatMap, MatchFlatMap2,
      MatchEquiJoin, MatchCross, MatchResidual)

    /** Creates a ValDef, and returns its Ident on the left hand side. */
    private def valRefAndDef(own: u.Symbol, name: String, rhs: u.Tree): (u.Ident, u.ValDef) = {
      val lhs = api.ValSym(own, api.TermName.fresh(name), rhs.tpe)
      (core.Ref(lhs), core.ValDef(lhs, rhs))
    }

    private def gens(qs: Seq[u.Tree]): Set[u.TermSymbol] =
      qs.collect { case cs.Generator(x, _) => x } (breakOut)
  }
}
