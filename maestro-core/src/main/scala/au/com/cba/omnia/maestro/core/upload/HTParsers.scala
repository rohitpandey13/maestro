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

package au.com.cba.omnia.maestro.core.upload

import au.com.cba.omnia.omnitool.Result
import au.com.cba.omnia.maestro.core.split.Splitter

/** Standard header information */
case class Header(businessDate: String, extractTime: String, fileName: String)

/** Standard trailer information */
case class Trailer(count: Long, checkValue: String, checkValColumn: String)

/** Parsers for data file header lines */
object HeaderParsers {
  /** Default header parser */
  val default: String => Result[Header] =
    (header: String) => Splitter.delimited("|").run(header) match {
      case List("H", busDate, extractTime, file) => Result.safe(Header(busDate, extractTime, file))
      case _                                     => Result.fail(s"Failed to parse header. Expected [H|<BusinessDate>|<ExtractTime>|<FileName>], but got [$header]")
    }
}

/** Parsers for data file trailer lines */
object TrailerParsers {
  /** Default trailer parser */
  val default: String => Result[Trailer] =
    (trailer: String) => Splitter.delimited("|").run(trailer) match {
      case List("T", count, checkVal, chkValCol) => Result.safe(Trailer(count.toLong, checkVal, chkValCol))
      case _                                     => Result.fail(s"Failed to parse trailer. Expected [T|<RecordCount>|<CheckSumValue>|<CheckSumColumn>], but got [$trailer]")
    }
}
