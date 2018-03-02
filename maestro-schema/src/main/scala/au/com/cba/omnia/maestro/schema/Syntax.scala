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

import au.com.cba.omnia.maestro.schema.syntax._


/** A concrete syntax that we identify as something more specific than 
 *  a random collection of characters.
 * 
 *  A syntax may describe a single Tope, though each Tope can have many syntaxes.
 *
 *  A syntax may not describe a Tope at all. For example, if some field is 
 *  always a collection of digits then we identify that as a specific syntax 
 *  without knowing what external entity it refers to.
 */
abstract class Syntax {

  /** Printable name for a syntax.
      This can be displayed to the user, as well as used for a key for maps. */
  def name: String

  /** Sort order to use when printing schemas.
      Lower is earlier. */
  def sortOrder: Int = 0

  /** Yield how well the syntax matches this String.
   *  In the range [0.0, 1.0].
   *  If 0.0 it definately does not match, if 1.0 it definately does. */
  def likeness(s: String): Double

  /** Check if this string completely matches the Tope, or not. */
  def matches(s: String): Boolean =
    likeness(s) >= 1.0

  /** If this syntax matches the given string, then so do these parent syntaxes.
   *  This information is used in the squashParents process, to remove
   *  parent nodes where there is a child node with the same count. */
  val parents: Set[Syntax]

  /** These child syntaxes are non-overlapping and completely partition
   * this parent syntax.
   * This information is used in the squashPartitions process, to remove
   * child nodes that exactly partition a parent node. */
  val partitions: Set[Syntax]

  /** Values that match this syntax are guaranteed not to match 
      with any other syntax. */
  val isIsolate: Boolean = 
    false

}


/** All the syntaxes we support, and functions on them. */
object Syntaxes {
  
  /** All the generic syntaxes that we know about.
   *  These are standard formats and encodings, that we use when we can't
   *  identify what Tope a String refers to. */
  val syntaxes  = Array(
    Any,
    Empty,
    Null,
    White,
    Upper,
    Lower,
    Alpha,
    AlphaNum,
    Integer,
    Nat,
    NegInt, 
    Real,
    NegReal,
    PosReal,

    // Null disasters.
    Exact("NULL"),
    Exact("null"),

    // Stupid placeholders.
    Exact("999999999"),
    Exact("99999"),
    Exact("99999.99"),
    Exact("XXXXXXXX"),

    // Timestamp sentinals.
    Exact("0000-00-00"),
    Exact("0001-01-01"),
    Exact("1900-01-01"),
    Exact("9999-12-31"),

    // Bool encodings.
    Exact("0"),
    Exact("1"),
    Exact("Y"),
    Exact("N"),
    Exact("ACTIVE"),
    Exact("INACTIVE"),
    Exact("active"),
    Exact("inactive"))

  /** All the syntaxes that are associated with topes. */
  val topeSyntaxes = Array(
    CbaSystemCode,
    CbaAccountCode,
    CurrencyCode,

    YYYYcMMcDD("."),
    YYYYcMMcDD("-"),
    YYYYcMMcDD("/"),

    DDcMMcYYYY("."),
    DDcMMcYYYY("-"),
    DDcMMcYYYY("/")
  )

  /** The names of all the syntaxes. */
  val names: Array[String] = 
    syntaxes .map {_.name}


  /** Get the partitions of a syntax. */
  def partitions(sParent: Syntax): Set[Syntax] = 
    sParent.partitions


  /** Get all the direct parents of a syntax. */
  def parents(s: Syntax): Set[Syntax] = 
    s.parents


  /** Get all the ancestors of a syntax. */
  def ancestors(s: Syntax): Set[Syntax] = {

    def widen(ss: Set[Syntax]): Set[Syntax] = {
      val ss2 = ss ++ ss.flatMap(s => parents(s))
      if (ss2.size == ss.size) ss
      else widen(ss2)
    }

    widen(parents(s))
  }


  /** Check whether these two syntaxes are siblings, 
   *  meaning they share a parent. */
  def areSiblings(s1: Syntax, s2: Syntax): Boolean = 
    (parents(s1) .intersect (parents(s2))) .size > 0


  /** Check whether all syntaxes in a set are siblings,
   *  meaning they share a parent. */
  def areSiblings(ss: Set[Syntax]): Boolean =
    ss .forall { s1 => ss .forall { s2 => areSiblings(s1, s2) } }


  /** Get the direct children of a syntax. 
   *  As each syntax only directly records its *parents*, when computing the
   *  children of a given syntax we need to scan through the full set, looking
   *  for for elements that have this syntax as their child. */
  def children(sParent: Syntax): Set[Syntax] =
    (syntaxes ++ topeSyntaxes)
      .filter { s => s.parents.contains(sParent) }
      .toSet


  /** Get all the descendants of a syntax. */
  def descendants(sParent: Syntax): Set[Syntax] = {

    def widen(ss: Set[Syntax]): Set[Syntax] = {
      val ss2 = ss ++ ss.flatMap(s => children(s))
      if (ss2.size == ss.size) ss
      else widen(ss2)
    }

    widen(children(sParent))
  }

  /** Get whether these two syntaxes are separate, meaning that the sets of
      strings in each syntax are disjoint. */
  def separate(s1: Syntax, s2: Syntax): Boolean = 
    (s1, s2) match { 
      case (Exact(str), _) => !s2.matches(str)
      case (_, Exact(str)) => !s1.matches(str)
      case _               => (s1 != s2) && (s1.isIsolate || s2.isIsolate)
   }
}

