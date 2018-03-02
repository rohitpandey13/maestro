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

import java.util.regex.Pattern

import scala.util.matching.Regex

/** Regular expressions for control files */
object ControlPattern {

  /** The default pattern used if the user does not specify their own pattern */
  val default: Regex = """S_.*|.*[_\.][Cc][Tt][RrLl]""".r

  /** Specify a control file pattern that matches files with given extensions */
  def extensions(exts: String*): Regex = {
    val extRegex  = exts.map(Pattern.quote(_).toString).mkString("(", "|", ")")
    s".*\\.$extRegex".r
  }
}
