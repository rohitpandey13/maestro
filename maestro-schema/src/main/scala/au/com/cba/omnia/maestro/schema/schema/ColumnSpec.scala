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

import au.com.cba.omnia.maestro.schema.hive._
import au.com.cba.omnia.maestro.schema.pretty._


/** Column specification.
 *
 *  @param name      column name
 *  @param hivetype  hive storage type (eg String)
 *  @param format    data format / semantic type for the column
 *  @param histogram histogram of how many values match each classifier
 *  @param comment   comment associated with the column
 */
case class ColumnSpec(
  name:        String,
  hivetype:    HiveType.HiveType,
  format:      Option[Format],
  histogram:   Histogram,
  comment:     String) {

  /** Check if this value matches the column format. */
  def matches(s: String): Boolean =
    format match {
      case None     => true
      case Some(f)  => f.matches(s)
    }


  /** Pretty print the ColumnSpec as a String. */
  def pretty: String =
    f"$name%-25s"               + " | "  +
    HiveType.pretty(hivetype)   + " | "  +
    prettyOptionFormat(format)  + "\n"   +
    " " * 25                    + " | "  +
    histogram.pretty + ";\n"


  /** Pretty print a Format as String, or '-' for a missing format. */
  def prettyOptionFormat(of: Option[Format]): String =
    of match {
      case None     => "-"
      case Some(f)  => f.pretty
    }


  /** Convert the ColumnSpec to JSON. */
  def toJson: JsonDoc = {

    val fields = List(
      Some(("name",      JsonString(name))),
      Some(("storage",   JsonString(HiveType.pretty(hivetype)))),
      Some(("format",    JsonString(prettyOptionFormat(format)))),
      Some(("histogram", histogram.toJson)),
      (if (comment.size > 0)
        Some (("comment", JsonString(comment)))
       else None)).flatten

    JsonMap(fields, true)
  }
}
