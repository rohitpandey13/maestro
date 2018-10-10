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

package au.com.cba.omnia.maestro.core
package clean

import au.com.cba.omnia.maestro.core.data._

case class Clean(run: (Field[_, _], String) => String) {
  /** Returns a new Clean instance that applied the current clean action ONLY if the condition evaluated to true. */
  def applyTo(conditionFn: Field[_, _] => Boolean): Clean =
    Clean((field, data) => if (conditionFn(field)) run(field, data) else data)
}

/** Remove control characters (including newlines) and/or
  * trim leading/trailing whitespace.
  *
  * <table>
  * <thead>
  * <tr><th><b>Option</b></th><th><b>Process</b></th><th><b>Characters</b></th></tr>
  * </thead>
  * <tr><td>posixDefault (legacy)</td><td>Trim then Clean</td><td>US-ASCII printables only</td></tr>
  * <tr><td>unicodeLegacyDefault</td><td>Trim then Clean</td><td>Plane 0 printables only</td></tr>
  * <tr><td>unicodeDefault</td><td>Clean then Trim</td><td>Plane 0 printables only</td></tr>
  * <tr><td>unicodeSupplementaryDefault</td><td>Clean then Trim</td><td>Plane 0 printables plus Surrogates in higher planes</td></tr>
  * </table>
  */
object Clean {

  /** Regex pattern to match supplementary plane surrogate characters (other than Private Use areas).
    * @see https://en.wikipedia.org/wiki/Universal_Character_Set_characters#Surrogates
    */
  val unicodeSupplementarySurrogatePattern = "\\uD800-\\uDB7F\\uDC00-\\uDFFF"

  def all(cleans: Clean*): Clean =
    Clean((field, data) => cleans.foldLeft(data)((acc, clean) => clean.run(field, acc)))

  def trim: Clean =
    Clean((_, data) => data.trim)

  /** Keep POSIX US-ASCII printables only.
    * @see https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html#posix
    */
  def removeNonPrintables: Clean =
    Clean((_, data) => data.replaceAll("[^\\p{Print}]", ""))

  /** Keep Unicode printables in Plane 0 (Basic Multilingual Plane) only.
    * @see http://www.unicode.org/reports/tr18/#Compatibility_Properties
    */
  def removeUnicodeNonPrintables: Clean =
    Clean((_, data) => data
      .replaceAll(s"[\\x{10000}-\\x{10FFFF}]", "")
      .replaceAll("(?U)[^\\p{Print}]", ""))

  /** Keep Unicode printables and supplementary (surrogate) characters like emoji,
    * but remove Private Use areas.
    */
  def removeUnicodeNonSupplementaryNonPrintables: Clean =
    Clean((_, data) => data.replaceAll(s"(?U)[^${unicodeSupplementarySurrogatePattern}\\p{Print}]", ""))

  /** See `posixDefault`. */
  def default: Clean = posixDefault

  /** Trim then keep POSIX US-ASCII printables only: this legacy posix support
    * will fail to trim spaces that are surrounded by removable control characters.
    * For example, "\u007f a" will be returned as " a".
    */
  def posixDefault: Clean = all(trim, removeNonPrintables)

  /** Keep Unicode Plane 0 printables only, then trim: default Unicode support
    * will filter out Plane 0 non-printables and higher-plane surrogates first,
    * then trim any remaining spaces.
    * For example, "\u007f a" will be returned as "a".
    *
    * Supports Plane 0 (Basic Multilingual Plane) only.
    * This Plane is widely supported by Unicode systems and
    * has a well-defined printable set.
    */
  def unicodeDefault: Clean = all(removeUnicodeNonPrintables, trim)

  /** Supplementary Unicode support will filter out Plane 0 non-printables and
    * higher-plane private-use surrogates first, then trim any remaining spaces.
    * For example, "\u007f a" will be returned as "a".
    *
    * Supports higher-plane supplementary surrogate characters,
    * except Private Use areas.
    * Surrogate characters may be mishandled by some Unicode systems
    * and higher planes are unallocated.
    * Therefore, this option can pose operational risks.
    */
  def unicodeSupplementaryDefault: Clean = all(removeUnicodeNonSupplementaryNonPrintables, trim)

  /** Trim then keep Unicode Plane 0 printables only: this legacy Unicode support
    * will fail to trim spaces that are surrounded by removable control characters.
    * For example, "\u007f a" will be returned as " a".
    *
    * Supports Plane 0 (Basic Multilingual Plane) only.
    * This Plane is widely supported by Unicode systems and
    * has a well-defined printable set.
    */
  def unicodeLegacyDefault: Clean = all(trim, removeUnicodeNonPrintables)

  /** Allow users to apply cleaners on selected fields */
  def applyTo(conditionFn: Field[_, _] => Boolean, cleaner: Clean): Clean =
    cleaner.applyTo(conditionFn)
}
