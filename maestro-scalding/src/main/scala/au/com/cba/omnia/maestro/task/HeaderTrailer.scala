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

import com.twitter.scalding.Execution

import org.apache.hadoop.fs.Path

import au.com.cba.omnia.omnitool.Result

import au.com.cba.omnia.permafrost.hdfs.Hdfs

import au.com.cba.omnia.maestro.core.upload.{HeaderParsers, TrailerParsers}
import au.com.cba.omnia.maestro.scalding.ExecutionOps._

/** Functions to parse the header and trailer from HDFS files. */
object HeaderTrailer {
  /**
    * Attempt to parse the first line of the file into a header data type.
    *
    * The default header parser is described in [[HeaderParsers]].
    * The user can supply their own parsing function and header data type if necessary.
    *
    * @tparam H Header data type
    *
    * @param parse Function to parse header
    * @param file   Input source file
    *
    * @return Header data
    */
  def parseHeader[A](parse: String => Result[A] = HeaderParsers.default)(path: Path): Execution[A] =
    Execution.fromHdfs(Hdfs.firstLine(path).andThen(parse))

  /**
    * Attempt to parse the last non empty line of the file into a trailer data type.
    *
    * The default trailer parser is described in [[TrailerParsers]].
    * The user can supply their own parsing function and trailer data type if necessary.
    *
    * @tparam T Trailer data type
    *
    * @param parse Function to parse trailer
    * @param file   Input source file
    *
    * @return Trailer data
    */
  def parseTrailer[A](parse: String => Result[A] = TrailerParsers.default)(path: Path): Execution[A] =
    Execution.fromHdfs(Hdfs.lastLine(path).andThen(parse))

  /**
    * Attempts to parse the first and last line of the file into header and trailer data types.
    *
    * The default parser functions assume that the header and trailer lines look like:
    *
    * {{{
    * H|<BusinessDate>|<ExtractTime>|<FileName>
    * }}}
    * {{{
    * T|<RecordCount>|<CheckSumValue>|<CheckSumColumn>
    * }}}
    *
    * The user can supply their own parsing functions and types if required.
    *
    * @tparam H Header data type
    * @tparam T Trailer data type
    *
    * @param parseHeader Function that parses the header
    * @param parseTrailer Function that parses the trailer
    *
    * @return Header and trailer data
    */
  def parseHeaderTrailer[H, T](
    parseHeader: String  => Result[H] = HeaderParsers.default,
    parseTrailer: String => Result[T] = TrailerParsers.default
  )(path: Path): Execution[(H, T)] =
    Execution.fromHdfs(Hdfs.firstLastLine(path).andThen { case (fl, ll) => for {
      h <- parseHeader(fl)
      t <- parseTrailer(ll)
    } yield (h, t) })
}
