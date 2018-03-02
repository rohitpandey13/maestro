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


/** Table Specification.
 *
 *  @param database    database name
 *  @param table       table name
 *  @param ignore      defines what rows to ignore in the input, eg for header fields
 *  @param columnSpecs specifications for each column
 */
case class TableSpec(
  database:    String,
  table:       String,
  ignore:      List[Ignore],
  columnSpecs: List[ColumnSpec]) {

  /** Pretty print a TableSpec as a String. */
  def pretty: String =
    columnSpecs .map (c => c.pretty)
      .mkString("\n")

  /** Convert the TableSpec to JSON. */
  def toJson: JsonDoc =
    JsonMap(List(
      ("database",  JsonString(database)),
      ("table",     JsonString(table)),
      ("columns",   JsonList(columnSpecs.map { _.toJson }, true))),
      true)
}
