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
package syntax

import  au.com.cba.omnia.maestro.schema._


/** A day in YYYYcMMcDD format with some separating character.
    Hard coded to only accept days in years 1800 through 2200. */
case class YYYYcMMcDD(sep: String) extends Syntax {

  val name  = s"YYYYcMMcDD(\'${sep}\')"

  def likeness(s: String) = 
    if   (s.length != 10) 0.0
    else {
      val sYear   = s.substring(0, 4)
      val sMonth  = s.substring(5, 7)
      val sDay    = s.substring(8, 10)

      if (sYear .forall(_.isDigit)  &&
          sMonth.forall(_.isDigit)  &&
          sDay  .forall(_.isDigit)  &&
          s(4).toString == sep      &&
          s(7).toString == sep)

        if (tope.Day.validishYearMonthDay(
              sYear.toInt, sMonth.toInt, sDay.toInt)) 
          1.0 else 0.0

      else 0.0
    }

  val parents:    Set[Syntax] = Set(Any)
  val partitions: Set[Syntax] = Set()
}


/** A day in DDcMMcYYYY format with some separating character.
    Hard coded to only accept days in years 1800 through 2200. */
case class DDcMMcYYYY(sep: String) extends Syntax {
  
  val name  = s"DDcMMcYYYY(\'${sep}\')"

  def likeness (s : String) = 
    if   (s.length != 10) 0.0
    else {
      val sDay    = s.substring(0, 2)
      val sMonth  = s.substring(3, 5)
      val sYear   = s.substring(6, 10)

      if (sYear .forall(_.isDigit)  &&
          sMonth.forall(_.isDigit)  &&
          sDay  .forall(_.isDigit)  &&
          s(2).toString == sep      &&
          s(5).toString == sep)

        if (tope.Day.validishYearMonthDay(
              sYear.toInt, sMonth.toInt, sDay.toInt))
          1.0 else 0.0

      else 0.0
    }

  val parents:    Set[Syntax] = Set(Any)
  val partitions: Set[Syntax] = Set()
}

