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
package validate

import org.scalacheck.{Arbitrary, Gen}

import scalaz._, \&/._

import org.joda.time.format.DateTimeFormat

import au.com.cba.omnia.omnitool.{Result, Ok, Error}

import au.com.cba.omnia.maestro.core.test.Spec

object CheckSpec extends Spec { def is = s2"""

Check properties
================

  Check.oneOf                        $oneOf
  Check.oneOf failure                $oneOfFailure
  Check.formatIncludesTimeTrue       $formatIncludesTimeTrue
  Check.formatIncludesTimeFalse      $formatIncludesTimeFalse
  Check.isDateNoHour                 $isDateNoHour
  Check.isDateNoTZ                   $isDateNoTZ
  Check.isDateDaylightSavingsSkip    $isDateDaylightSavingsSkip
  Check.isDateNoDaylightSavingsSkip  $isDateNoDaylightSavingsSkip
  Check.isDateUTC                    $isDateUTC
  Check.nonempty                     $nonempty
  Check.nonempty failure             $nonemptyFailure

"""
  def categories = List("A", "B", "C", "D")
  case class Category(value: String)

  def oneOf = prop((c: Category) =>
    Check.oneOf(categories:_*).run(c.value) must_== Result.ok(c.value))

  def oneOfFailure = prop((c: Category) =>
    Check.oneOf(categories:_*).run(c.value + "bad") must beLike {
      case Ok(_) => ko
      case _     => ok
    })

  def formatIncludesTimeTrue =
    Check.formatIncludesTime(DateTimeFormat.forPattern("y-M-d-H")) must_== true

  def formatIncludesTimeFalse =
    Check.formatIncludesTime(DateTimeFormat.forPattern("y-M-d")) must_== false

  def isDateNoHour =
    Check.isDate("y-M-d") must not(throwA[Exception])

  def isDateNoTZ =
    Check.isDate("y-M-d-H") must throwA[Exception]

  def isDateDaylightSavingsSkip =
    Check.isDateSydney("y-M-d-H-m").run("2016-10-02-02-30") must_==
      Error(This("Date 2016-10-02-02-30 is not a valid date in the format y-M-d-H-m in Australia/Sydney"))

  def isDateNoDaylightSavingsSkip =
    Check.isDateSydney("y-M-d-H-m").run("2016-10-02-03-30") must_== Ok("2016-10-02-03-30")

  def isDateUTC =        // The same date time string as the daylight savings skip in Sydney.
    Check.isDateUTC("y-M-d-H-m").run("2016-10-02-02-30")  must_== Ok("2016-10-02-02-30")

  def nonempty = prop((s: String) => !s.isEmpty ==> {
    Check.nonempty.run(s) must_== Result.ok(s) })

  def nonemptyFailure =
    Check.nonempty.run("") must beLike {
      case Ok(_) => ko
      case _     => ok
    }

  implicit def CategoryArbitrary: Arbitrary[Category] =
    Arbitrary(Gen.oneOf(categories) map Category)
}
