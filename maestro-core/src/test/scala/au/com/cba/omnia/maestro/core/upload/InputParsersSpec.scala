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
package upload

import org.joda.time.{DateTime, DateTimeZone, Period}

import org.specs2.Specification
import org.specs2.matcher.ThrownExpectations

import au.com.cba.omnia.omnitool.{Ok, Error}


class InputParsersSpec extends Specification with ThrownExpectations { def is = s2"""
successfull file pattern parsing
--------------------------------

  can parse {table}{yyyyMMdd}             $parseUpToDay
  can parse {table}{yyyyMMddHHmmss}       $parseUpToSecond
  can parse {table}{yyyyddMM}             $parseDifferentFieldOrder
  can parse {table}{yyMMdd}               $parseShortYear
  can parse {table}_{yyyy-MM-dd-HH}       $parseLiterals
  can parse {ddMMyyyy}{table}             $parseDifferentElementOrder
  can parse {table}*{yyyyMMdd}*           $parseWildcards
  can parse {table}??{yyyyMMdd}           $parseQuestionMarks
  can parse {table}_{yyyyMMdd}_{MMyyyy}   $parseDuplicateTimestamps
  can parse {yyyy}_{table}_{MMdd}         $parseCombinedTimestampFields

failing file pattern parsing
----------------------------

  fail on {table}                         $expectOneTimestamp
  fail on {table}{yyyydd}                 $expectContiguousFields
  fail on {table}{MMdd}                   $expectYear
  fail on conflicting field values        $expectConstantFieldValue
  fail on invalid field values            $expectValidFieldValues
"""

  // Helper methods
  def forPatternUTC = InputParsers.forPattern(DateTimeZone.UTC) _
  def utc(y: Int, mon: Int, d: Int, h: Int = 0, min: Int = 0, s: Int = 0) =
    new DateTime(y, mon, d, h, min, s, DateTimeZone.UTC)

  val sydneyTZ = DateTimeZone.forID("Australia/Sydney")

  val (oneYear, oneMonth, oneDay, oneHour, oneMinute, oneSecond) = (
    Period.years(1), Period.months(1), Period.days(1), Period.hours(1), Period.minutes(1), Period.seconds(1)
  )

  def parseUpToDay =
    forPatternUTC("mytable", "{table}{yyyyMMdd}") must beLike {
      case Ok(matcher) => {
        matcher("mytable20140807") must_== Ok(Match(List("2014", "08", "07"), utc(2014, 8, 7),  oneDay))
        matcher("mytable20140830") must_== Ok(Match(List("2014", "08", "30"), utc(2014, 8, 30), oneDay))
      }
    }

  def parseUpToSecond =
    forPatternUTC("mytable", "{table}{yyyyMMddHHmmss}") must beLike {
      case Ok(matcher) => {
        matcher("mytable20140807203000") must_==
          Ok(Match(List("2014", "08", "07", "20", "30", "00"), utc(2014, 8, 7, 20, 30, 0), oneSecond))
      }
    }

  def parseDifferentFieldOrder =
    forPatternUTC("foobar", "{table}{yyyyddMM}") must beLike {
      case Ok(matcher) => {
        matcher("foobar20140708") must_== Ok(Match(List("2014", "08", "07"), utc(2014, 8, 7), oneDay))
      }
    }

  def parseShortYear =
    forPatternUTC("foobar", "{table}{yyMMdd}") must beLike {
      case Ok(matcher) => {
        matcher("foobar140807") must_== Ok(Match(List("2014", "08", "07"), utc(2014, 8, 7), oneDay))
      }
    }

  def parseLiterals =
    forPatternUTC("credit", "{table}_{yyyy-MM-dd-HH}") must beLike {
      case Ok(matcher) => {
        matcher("credit_2014-08-07-20") must_== Ok(Match(List("2014", "08", "07", "20"), utc(2014, 8, 7, 20), oneHour))
      }
    }

  def parseDifferentElementOrder =
    forPatternUTC("credit", "{ddMMyyyy}{table}") must beLike {
      case Ok(matcher) => {
        matcher("07082014credit") must_== Ok(Match(List("2014", "08", "07"), utc(2014, 8, 7), oneDay))
      }
    }

  def parseWildcards =
    forPatternUTC("mytable", "{table}*{yyyyMMdd}*") must beLike {
      case Ok(matcher) => {
        matcher("mytable-foobar-2014-201408079999.foobar") must_==
          Ok(Match(List("2014", "08", "07"), utc(2014, 8, 7), oneDay))
      }
    }

  def parseQuestionMarks =
    forPatternUTC("mytable", "{table}??{yyyyMMdd}") must beLike {
      case Ok(matcher) => {
        matcher("mytable--20140807") must_== Ok(Match(List("2014", "08", "07"), utc(2014, 8, 7), oneDay))
        matcher("mytable0020140807") must_== Ok(Match(List("2014", "08", "07"), utc(2014, 8, 7), oneDay))
      }
    }

  def parseDuplicateTimestamps =
    forPatternUTC("cars", "{table}_{yyyyMMdd}_{MMyyyy}") must beLike {
      case Ok(matcher) => {
        matcher("cars_20140807_082014") must_== Ok(Match(List("2014", "08", "07"), utc(2014, 8, 7), oneDay))
      }
    }

  def parseCombinedTimestampFields =
    forPatternUTC("cars", "{yyyy}_{table}_{MMdd}") must beLike {
      case Ok(matcher) => {
        matcher("2014_cars_0807") must_== Ok(Match(List("2014", "08", "07"), utc(2014, 8, 7), oneDay))
      }
    }

  def expectOneTimestamp =
    forPatternUTC("marketing", "{table}") must beLike {
      case Error(_) => ok
    }

  def expectContiguousFields =
    forPatternUTC("marketing", "{table}{yyyydd}") must beLike {
      case Error(_) => ok
    }

  def expectYear =
    forPatternUTC("marketing", "{table}{MMdd}") must beLike {
      case Error(_) => ok
    }

  def expectConstantFieldValue =
    forPatternUTC("dummy", "{table}_{yyMM}_{yyMM}") must beLike {
      case Ok(matcher) => {
        matcher("dummy_1408_1401") must beLike { case Error(_) => ok }
      }
    }

  def expectValidFieldValues =
    forPatternUTC("dummy", "{table}{yyyyMMdd}") must beLike {
      case Ok(matcher) => {
        matcher("dummy20140231") must beLike { case Error(_) => ok }
      }
    }

  def parseLiteralsSydney =
    InputParsers.forPattern(sydneyTZ)("credit", "{table}_{yyyy-MM-dd-HH}") must beLike {
      case Ok(matcher) => {
        matcher("credit_2014-08-07-20") must_==
          Ok(Match(List("2014", "08", "07", "20"), new DateTime(2014, 8, 7, 20, 0, 0, sydneyTZ), oneHour))
      }
    }
}
