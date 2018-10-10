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

import au.com.cba.omnia.maestro.schema.tope._


/** A Tope is a thing in the world that we're trying to describe with a
    String. Each Tope can have several possible syntaxes. */
abstract class Tope {

  /** Printable name for a Tope. */
  def name: String

  /** Printable names for the posible syntaxes of a Tope. */
  def names: Array[String] =
    syntaxes
      .map { syntax => name + "." + syntax.name }

  /** The possible syntaxes of a Tope. */
  def syntaxes: Array[Syntax]

  /** Yield how likely it is that the string refers to this Tope.
   * In the range [0.0, 1.0].
   * If 0.0 it definately does not match, if 1.0 it definately does. */
  def likeness (s: String): Array[Double]
}


/** All the Topes that we support, and functions on them. */
object Topes {

  /** All the Topes that we know about. */
  val topes     = Array(
    Day,
    Currency,
    CbaSystem,
    CbaAccount)

  /** All the Topes with their possible syntaxes. */
  val topeSyntaxes: Array[(Tope, Syntax)] =
    topes.flatMap { t: Tope => t.syntaxes.map { s => (t, s) } }

  /** Names of all the Topes, like "Day". */
  val names: Array[String] =
    topes .map { _.name }

  /** Names of all the Tope syntaxes, like "Day.YYYYcMMcDD('-')" */
  val namesSyntaxes: Array[String] =
    topes .flatMap { _.names }
}
