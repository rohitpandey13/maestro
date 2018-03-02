//   Copyright 2017 Commonwealth Bank of Australia
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

import org.scalacheck.Prop

import org.specs2.matcher.Matcher

import au.com.cba.omnia.maestro.core.data.Field

object CleanSpec extends test.Spec { def is = s2"""

Clean properties
================

  US-ASCII is handled the same by posixDefault and unicodeDefault      $ascii
  default trims the same way it always has (no regression)             $default
  posixDefault trims to POSIX US-ASCII Printable chars only            $posixDefault
  posixDefault trims before cleaning (legacy behaviour)                $posixDefaultTrim
  unicodeLegacyDefault trims then cleans                               $unicodeLegacyDefaultTrim
  unicodeDefault cleans then trims                                     $unicodeDefaultTrim
  unicodeDefault trims to Unicode TR#18 Annex C Printable chars only   $unicodeDefault
  unicodeDefault accepts windows-1252 printable characters             $windows1252
  unicodeDefault removes windows-1252 control characters               $windows1252clean
  unicodeDefault accepts printable basic plane 0 characters            $unicodeDefaultPlane0
  unicodeDefault removes printable supplementary plane 1 characters    $unicodeDefaultPlane1
  unicodeDefault removes printable ideographic plane 2 characters      $unicodeDefaultPlane2
  unicodeDefault whitespace                                            $unicodeDefaultWhitespace
  unicodeSupplementaryDefault trims printables and surrogates          $unicodeSupplementaryDefault
  unicodeSupplementaryDefault accepts basic plane 0 printable chars    $unicodeSupplementaryDefaultPlane0
  unicodeSupplementaryDefault accepts multilingual plane 1 characters  $unicodeSupplementaryDefaultPlane1
  unicodeSupplementaryDefault accepts ideographic plane 2 characters   $unicodeSupplementaryDefaultPlane2
  unicodeSupplementaryDefault whitespace                               $unicodeSupplementaryDefaultWhitespace

"""

  case class Whitespace(name: String, input: String, output: String)

  // Unicode whitespace regression test
  val unicodeWhitespace = Set(
    Whitespace("US-ASCII NUL",        "\u0000",  ""      ),
    Whitespace("US-ASCII Space",      " ",       ""      ),
    Whitespace("US-ASCII LF",         "\n",      ""      ),
    Whitespace("US-ASCII CR",         "\r",      ""      ),
    Whitespace("Non-breaking space",  "\u00a0",  "\u00a0"),
    Whitespace("Zero-width space",    "\u200b",  "\u200b"),
    Whitespace("Delete",              "\u007f",  ""      )
  )

  val f = Field("dummy", (s: String) => s)

  val allAscii = (0 to 127).map(_.toChar).mkString

  val posixPrintable =
    " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"

  val isPosixPrintable = posixPrintable.toSet

  val windows1252Printable =
    "!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~€‚ƒ„…†‡ˆ‰Š‹ŒŽ‘’“”•–—˜™š›œžŸ ¡¢£¤¥¦§¨©ª«¬­®¯°±²³´µ¶·¸¹º»¼½¾¿ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖ×ØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ"

  val windows1252NonPrintable =
    ((0 to 31) ++ Seq(129, 141, 143, 144, 157)).map(_.toChar).mkString

  val trimmableEmbeddedSpace = "\u007f a\n "

  val unicodePlane0 = "Á®—÷“…”αあ大❤️" // basic languages and symbols

  val unicodePlane1 = "𐀀𝄞😀🤖" // surrogates e.g. emoji

  val unicodePlane2 = "𠀋" // extended CJK

  def isUnicodePrintable(c: Char): Boolean =
    c.toString.matches("(?U)[\\p{Graph}\\p{Blank}&&[^\\p{Cntrl}]]")

  def isUnicodePrintableWithSurrogates(c: Char): Boolean =
    c.toString.matches(s"(?U)[\\p{Graph}\\p{Blank}${Clean.unicodeSupplementarySurrogatePattern}&&[^\\p{Cntrl}]]")

  def ascii = {
    val posixResult = Clean.posixDefault.run(f, allAscii)
    val unicodeResult = Clean.unicodeDefault.run(f, allAscii)
    val expectedResult = posixPrintable.mkString.trim
    // three-way comparison:
    Set(posixResult, unicodeResult) must_== Set(expectedResult)
  }

  def default = {
    val testRange = (0 to 255).map(_.toChar).mkString
    val oldDefault = Clean((_, data) => data.trim.replaceAll("[^\\p{Print}]", "")) // traditional implementation
    Clean.default.run(f, testRange) must_== oldDefault.run(f, testRange)
  }

  def posixDefault = Prop.forAll { (str: String) =>
    Clean.posixDefault.run(f, str) must_== str.trim.filter(isPosixPrintable)
  }

  def posixDefaultTrim = {
    Clean.posixDefault.run(f, trimmableEmbeddedSpace) must_== " a"
  }

  def unicodeDefault = Prop.forAll { (str: String) =>
    Clean.unicodeDefault.run(f, str) must_== str.trim.filter(isUnicodePrintable)
  }

  def unicodeDefaultTrim = {
    Clean.unicodeDefault.run(f, trimmableEmbeddedSpace) must_== "a"
  }

  def unicodeLegacyDefaultTrim = {
    Clean.unicodeLegacyDefault.run(f, trimmableEmbeddedSpace) must_== " a"
  }

  def windows1252 = {
    Clean.unicodeDefault.run(f, windows1252Printable) must_== windows1252Printable
  }

  def windows1252clean = {
    Clean.unicodeDefault.run(f, windows1252NonPrintable) must beEmpty
  }

  def unicodeDefaultPlane0 = {
    Clean.unicodeDefault.run(f, unicodePlane0) must_== unicodePlane0
  }

  def unicodeDefaultPlane1 = {
    Clean.unicodeDefault.run(f, unicodePlane1) must beEmpty
  }

  def unicodeDefaultPlane2 = {
    Clean.unicodeDefault.run(f, unicodePlane2) must beEmpty
  }

  def unicodeSupplementaryDefault = Prop.forAll { (str: String) =>
    Clean.unicodeSupplementaryDefault.run(f, str) must_== str.trim.filter(isUnicodePrintableWithSurrogates)
  }

  def unicodeSupplementaryDefaultPlane0 = {
    Clean.unicodeSupplementaryDefault.run(f, unicodePlane0) must_== unicodePlane0
  }

  def unicodeSupplementaryDefaultPlane1 = {
    Clean.unicodeSupplementaryDefault.run(f, unicodePlane1) must_== unicodePlane1
  }

  def unicodeSupplementaryDefaultPlane2 = {
    Clean.unicodeSupplementaryDefault.run(f, unicodePlane2) must_== unicodePlane2
  }

  def testWhitespace(clean: Clean) = {
    def expectation: Matcher[(Whitespace, String)] = { (_: (Whitespace, String)) match {
      case (ws, clean) => (ws.output == clean) -> ws.name // pretty specs2 message
    }}
    val results = unicodeWhitespace.map(ws => ws -> clean.run(f, ws.input))
    results must contain(expectation).foreach
  }

  def unicodeDefaultWhitespace = testWhitespace(Clean.unicodeDefault)

  def unicodeSupplementaryDefaultWhitespace = testWhitespace(Clean.unicodeSupplementaryDefault)
}
