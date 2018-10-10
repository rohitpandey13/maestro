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

import au.com.cba.omnia.maestro.schema.pretty._


/** Data format / semantic type of a column.
 *  @param list All data values must match one of these classifiers.
 */
case class Format(list: List[Classifier]) {

  /** Append two formats. */
  def ++(that: Format): Format =
    Format(this.list ++ that.list)

  /** Check if this string matches any of the classifiers in the Format. */
  def matches(s: String): Boolean =
    list.exists(_.matches(s))

  /** Pretty print a Format as a string. */
  def pretty: String =
    list
      .map {_.name}
      .mkString (" + ")

  /** Convert the format to a JSON. */
  def toJson: JsonDoc =
    JsonString(pretty)
}
