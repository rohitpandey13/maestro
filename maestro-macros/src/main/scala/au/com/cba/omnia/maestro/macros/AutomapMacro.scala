//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.maestro.macros

import scala.annotation.StaticAnnotation
import scala.reflect.macros.TypecheckException
import scala.reflect.macros.whitebox.Context
import scala.language.experimental.macros
import scala.collection.immutable.StringOps

import scalaz.Scalaz._
import scalaz.NonEmptyList
import scalaz.\/

/**
  * Automapper for Thrift structs.
  *
  * One or more input structs are mapped to a single output struct, using a custom syntax to
  * define mapping rules for each field. Output struct fields for which mapping rules are
  * unspecified are mapped to matching fields with the same name and type in the input structs.
  * If more than one such input candidate exists, all structs with a field by this name must
  * agree on its value, otherwise a runtime exception is raised.
  *
  * To use the automapper, declare a method with the desired signature, assign to it a block of
  * transformation rules and annotate it with @automapper. The macro fills in the method with the
  * required logic. A simple use of the automapper annotation macro might look like this:
  * 
  * {{{
  *   @automap def mkFooBar(foo: Foo, bar: Bar): FooBar = {
  *       thisInt    := foo.thatInt
  *       thisString := bar.thatString
  *   }
  * }}}
  *
  * Note that the macro will create a function `mkFooBar: ((Foo, Bar)) => FooBar`. This is different
  * from the original `mkFooBar`` declaration which took two arguments, whereas the generated method
  * takes a tuple. This is merely a syntactic convenience: it is clearer to write mapping rules in
  * terms of named arguments, but Scalding pipes work with tuples. The Automapper is intended to
  * help bridge that gap.
  */
object AutomapMacro {
  def impl(c: Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._
    /** Abort macro expansion with an error message. */
    def fail(reason: String) = c.abort(c.enclosingPosition, reason)

    /** Ugly hack to convert the type that is in `tree` to an instance of [[Type]]. */
    def resolveType(tree: Tree): Type = {
      // The following obscure construction seems to be the simplest way of reliably forcing
      // resolution of an externally-defined class and its companion class.
      val q"{ class $_[$_] { type FakeType = $typ; () } }" =
        c.typecheck( q"{ class Dummy[T] { type FakeType = $tree; () } }" )

      typ.tpe
    }

    /**
      * For multiple sources, produces code like (modulo whitespace, fully qualified paths, and variable names):
      *
      *  {{{
      *  if (x.field1 == y.field1 && x.field1 == z.field1) x.field1
      *  else
      *     val details = 
      *     throw new IllegalArgumentException("Ambiguous source values for field1: " + List("x.field1 = " + x.field1, "y.field1 = " + y.field1, "z.field1 = " + z.field1).mkString(", "))
      *  }}}
      */
    def disambiguateValues(name: String, values: NonEmptyList[(TermName, Tree)]): Tree = {
      val (hn, ht)  = values.head
      val conditions =
        values.tail
          .map(t => q"$ht == ${t._2}")

      val condition =
        if (conditions.length == 1) conditions.head
        else                        conditions.reduce((c1, c2) => q"$c1 && $c2")

      val errorDetails = values.map { case (src, v) =>
        val srcString = s"$src.$name = "
        q"$srcString + $v"
      }.list

      val exceptionMsg = s"Ambiguous source values for $name: "

      q"""
        if ($condition) $ht
        else
          throw new IllegalArgumentException($exceptionMsg + List(..$errorDetails).mkString(", "))
      """
    }

    /**
      * Parses all the manually specified rules.
      * 
      * Parses rules of the form `n := <code block>` where `n` is the name of a field in the target
      * thrift structure.
      * This fdisction returns error messages for any failed override rules, fields that have more
      * than one override rule and a map of fields to the corresponding code for that field.
      */
    def processManualRules(
      rules: List[Tree], srcs: List[(TermName, Type)],
      dst: String, dstFields: Map[String, MethodSymbol]
    ): (List[String], List[String], Map[String, Tree]) = {
      // Extract all the manual code specified for each field.
      val (invalidOverrides, validOverrides) =
        rules
          .map { case q"$x := $expr" =>
            dstFields.get(x.toString).cata(_ =>
              (x.toString, expr).right,
              s"$x is not a member of $dst.".left
            )
        }.separate

      val overrides: Map[String, Tree] = validOverrides.toMap

      // Identify any fields for which we try to set multiple times
      val multipleOverrides: List[String] =
        rules.map { case q"$x := $_" => x }.groupBy(identity).collect {
          case (k, vs) if vs.length > 1 => k.toString
        }.toList


      (invalidOverrides, multipleOverrides, overrides)
    }

    /**
      * Tries to create assignments for all the remaining fields in the target based on
      * fields in the source with the same name and type.
      *
      * Returns the list of fields for which we were not able to create a default assingments
      * and a map of fields to the corresponding assignment code.
      */
    def processDefaults(
      fields: Map[String, MethodSymbol], srcs: List[(TermName, Type)]
    ): (List[String], Map[String, Tree]) = {
      val srcFields: List[(TermName, Map[String, MethodSymbol])] =
        srcs.map { case (n, t) => (n, Inspect.fieldsMapUnsafe(c)(t)) }

      //Tries and matches each of the remaining fields with fields from the inputs based on name and type.
      val defaults: Map[String, List[(TermName, Tree)]] = fields.map { case (name, method) =>
        (name, srcFields.flatMap { case (src, fields) =>
          fields.get(name)
            .filter(_.returnType == method.returnType)
            .map(srcField => (src, q"$src.$srcField"))
        })
      }

      // Identify all fields for which we don't have manual rules or matching fields in the sources.
      val missing: List[String] = defaults.toList.collect { case (n, List()) => n }

      // Code to assign a value from the input sources.
      // Where multiple sources had matched a particular field in type and name we
      // disambiguate the value at run time.
      val defaultsCode: Map[String, Tree] = defaults.collect {
        case (n, List((_, t))) => (n, t)
        case (n, h :: ts)      => (n, disambiguateValues(n, NonEmptyList.nel(h, ts)))
      }

      (missing, defaultsCode)
    }

    /** Process the automap instructions and create the corresponding automap fuction. */
    def mkMapper(src: Tree): String \/ Tree = {
      val q"def $name (..$srcArgs): $srcTo = { ..$rules }" = src
      val srcs: List[(TermName, Type)] =
        srcArgs.map { case q"$_ val $n: $t = $_" => (n, resolveType(t)) }
      val dst       = resolveType(srcTo)
      val dstFields = Inspect.fieldsMapUnsafe(c)(dst)

      val (invalidOverrides, multipleOverrides, overrides) =
        processManualRules(rules, srcs, dst.toString, dstFields)
      // Determine all the fields that don't have manual rules.
      val remainingFields: Map[String, MethodSymbol] =
        dstFields.filterKeys(k => !overrides.contains(k))
      val (missing, defaults) = processDefaults(remainingFields, srcs)

      if (invalidOverrides.nonEmpty || multipleOverrides.nonEmpty || missing.nonEmpty) {
        val multipleOverrideErrors =
          if (multipleOverrides.isEmpty) List.empty
          else List(s"""Got multiple manual definitions for these fields: ${multipleOverrides.mkString(", ")}.""")
        val missingErrors =
          if (missing.isEmpty) List.empty
          else List(s"""Got no default or manual value for the these fields: ${missing.mkString(", ")}.""")
        val errors = invalidOverrides ++ multipleOverrideErrors ++ missingErrors

        s"""Got errors trying to create automap for $dst. ${errors.mkString(" ")}""".left
      } else {
        val constructor = Inspect.constructNamedUnsafe(c)(dst, overrides.toList ++ defaults.toList)
        val vals = pq"""(..${srcs.map(x => pq"${x._1} @ _")})"""

        q"""def $name(in: (..${srcs.map(_._2)})): $dst = in match { case $vals => $constructor }""".right
      }
    }

    annottees
      .map(_.tree)
      .map {
        case (x: DefDef) => mkMapper(x).fold(fail, c.Expr[Any](_))
        case _           => fail("Automap annottee must be method accepting thrift structs and returning one.")
    }.headOption.getOrElse(fail("No annottees found"))
  }
}

/** Annotation to involve automap macro. See [[AutomapMacro]] for more detail. */
class automap(xs: Any*) extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro AutomapMacro.impl
}
