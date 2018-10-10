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
package tope

import  au.com.cba.omnia.maestro.schema._
import  au.com.cba.omnia.maestro.schema.syntax._


/** A Day. 
 *  The syntax recognisers are hard coded to only accept days in the year
 *  range 1800 through 2200, because we want to reject sential values
 *  like 0001-01-01 which do not refer to a real day. */
object Day extends Tope {
  
  val name  = "Day"

  def likeness(s: String): Array[Double] =
    syntaxes .map { _.likeness(s) }

  val syntaxes: Array[Syntax] = Array(
    YYYYcMMcDD("-"),
    YYYYcMMcDD("."),
    YYYYcMMcDD("/"),

    DDcMMcYYYY("-"),
    DDcMMcYYYY("."),
    DDcMMcYYYY("/"))


  // Valid day determinator. These use static unboxed arrays to store
  // the number of days in each month, because we're trying to avoid
  // boxing/unboxing overhead when validating dates.

  /** Days in each month of a leap year. */
  val monthDays_leap = Array(
    0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)

  /** Days in each month of a non-leap year. */
  val monthDays_noleap = Array(
    0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)

  /** Yield the number of days in each month of the year,
      depending on whether it was a leap year. */
  def monthDays(leap: Boolean): Array[Int] =
    if (leap) monthDays_leap else monthDays_noleap

  /** Check if this looks like a valid day descriptor. */
  def validishYearMonthDay(year: Int, month: Int, day: Int): Boolean =
    if (year  >= 1800 && year  <= 2200 &&
        month >= 1    && month <= 12)
       (day   >= 1    && day   <= monthDays(year % 4 == 0)(month))
    else false
}

