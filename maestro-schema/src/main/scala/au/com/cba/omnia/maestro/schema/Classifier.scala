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
package au.com.cba.omnia.maestro.schema


/** A wrapper for raw syntaxes, and topes with their syntax. */
sealed trait Classifier {
  /** Human readable name for the Classifier. */
  def name: String

  /** Sort order when pretty printing schemas. */
  def sortOrder: Int

  /** Yield how well a string matches the classifier, in the range 0.0 to 1.0.
   *  If 1.0 the string definately does match, if 0.0 it definately does not.
   *  The meaning of scores in between 0.0 and 1.0 depends on the particular
   *  classifier. */
  def likeness(s: String): Double

  /** Check if this string completely matches the classifier,
   *  meaning the likeness is 1.0 */
  def matches(s: String): Boolean =
    likeness(s) >= 1.0

  /** Yield the parent syntaxes of this classifier, if there are any.
   *  If a string matches this syntax, then it also matches all the parents. */
  def parents: Set[Syntax]
}


/** Wrap a Syntax as a Classifier. */
case class ClasSyntax(syntax: Syntax)
  extends Classifier {

  val name: String =
    syntax.name

  val sortOrder: Int =
    syntax.sortOrder

  def likeness(s: String): Double =
    syntax.likeness(s)

  def parents: Set[Syntax] =
    syntax.parents
}


/** Wrap a Tope along with its representation Syntax as a Classifier. */
case class ClasTope(tope: Tope, syntax: Syntax)
  extends Classifier {

  val name: String =
    tope.name + "." ++ syntax.name

  val sortOrder: Int =
    100

  def likeness(s: String): Double =
    syntax.likeness(s)

  def parents: Set[Syntax] =
    syntax.parents
}


/** Companion object for Classifiers. */
object Classifier {

  /** Yield an array of all classifiers we know about.
   *  We keep classifier counts in an array to ensure they are stored unboxed at
   *  runtime. The classifiers themselves are also stored in an array so we can
   *  just map arrays to arrays when performing the count. */
  val all: Array[Classifier] = {

    // Classifiers that wrap raw syntaxes.
    def sx: Array[Classifier] =
      Syntaxes.syntaxes
        .map { s => ClasSyntax(s) }.toArray

    // Classifiers that wrap topes.
    def st: Array[Classifier] =
      Topes.topeSyntaxes
        .map { case (tope, syntax) => ClasTope (tope, syntax) }

    sx ++ st
  }
}
