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

package au.com.cba.omnia.maestro.task

import scala.util.Failure

import au.com.cba.omnia.thermometer.core.ThermometerSpec
import au.com.cba.omnia.thermometer.core.Thermometer._

import au.com.cba.omnia.maestro.core.upload.{Header, Trailer}

object HeaderTrailerSpec extends ThermometerSpec { def is = s2"""

Header Trailer task
===================
  can parse  header and trailer and return right result   $headerTrailerParsing
  can parse header and return right result                $headerParsing
  can parse trailer and return right result               $trailerParsing
  can parse trailer with empty lines at the end of file   $trailerParsingWithEmptyLines
  parse garbage data and return result failure            $garbageParsing
"""

  def headerTrailerParsing = withEnvironment(path(getClass.getResource("/ht-parser").toString)) {
    val path   = "local-ingest/dataFeed/normal/mydomain/mytable_20141118.dat".toPath
    val result = (Header("02-10-2014", "2:00:00 pm", "mytable_20141118.dat"), Trailer(4, "034", "name"))

    executesSuccessfully(HeaderTrailer.parseHeaderTrailer()(path)) must_== result
  }

  def trailerParsing = withEnvironment(path(getClass.getResource("/trailer-parser").toString)) {
    val path   = "local-ingest/dataFeed/normal/mydomain/mytable_20141118.dat".toPath
    val result = Trailer(4, "034", "name")

    executesSuccessfully(HeaderTrailer.parseTrailer()(path)) must_== result
  }

  def trailerParsingWithEmptyLines = withEnvironment(path(getClass.getResource("/trailer-parser-empty-lines").toString)) {
    val path   = "local-ingest/dataFeed/normal/mydomain/mytable_20141118.dat".toPath
    val result = Trailer(4, "034", "name")

    executesSuccessfully(HeaderTrailer.parseTrailer()(path)) must_== result
  }

  def headerParsing = withEnvironment(path(getClass.getResource("/header-parser").toString)) {
    val path   = "local-ingest/dataFeed/normal/mydomain/mytable_20141118.dat".toPath
    val result = Header("02-10-2014", "2:00:00 pm", "mytable_20141118.dat")

    executesSuccessfully(HeaderTrailer.parseHeader()(path)) must_== result
  }

  def garbageParsing = withEnvironment(path(getClass.getResource("/garbage-parser").toString)) {
    val path = "local-ingest/dataFeed/normal/mydomain/mytable_20141118.dat".toPath

    execute(HeaderTrailer.parseHeader()(path)) must beLike { case Failure(_) => ok }
  }
}

