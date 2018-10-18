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
package commands

import com.quantifind.sumac.ArgMain
import com.quantifind.sumac.validation._
import com.quantifind.sumac.FieldArgs

import au.com.cba.omnia.maestro.schema
import au.com.cba.omnia.maestro.schema._
import au.com.cba.omnia.maestro.schema.pretty._
import au.com.cba.omnia.maestro.schema.taste.{Load => TasteLoad}


/** Arguments for `Infer` command. .*/
class InferArgs extends FieldArgs {

  /** Name of the database. */
  @Required var database: String = _

  /** Name of the table. */
  @Required var table: String = _

  /** File containing names and storage types of columns.
   *  Each line is treated as a description of a single column,
   *  where the first  word is the column name,
   *  and   the second word is the hive storage type.
   *
   *  Example:
   *    paty_i   string
   *    acct_i   string
   *    balance  float
   */
  var names: Option[String] = None

  /** Taste file, containing histogram of what type of data is in the columns. */
  @Required var taste: String = _
}


/** Infer a schema, based on the names file and histogram. */
object Infer extends ArgMain[InferArgs]
{
  def main(args: InferArgs): Unit = {

    // Load the names and histograms.
    // Also squash the hisograms so we have just the most specific classifiers.
    val taste = TasteLoad.loadNamesTaste(args.names, args.taste)
    val names = taste .map { _._1 }
    val hists = taste .map { _._2 } .map { h => schema.Squash.squash(h) }

    // Infer specs for all the columns.
    val colSpecs =
      names .zip (hists)
        .map { case (name, hist) =>
            ColumnSpec(
              name,
              hive.HiveType.HiveString,
              schema.Infer.infer(hist),
              hist,
              "") }

    // Build the overall spec for the table.
    val tableSpec: TableSpec =
      TableSpec(
        args.database,
        args.table,
        List(),
        colSpecs)

    // Print the JSONified table spec to console.
    println(JsonDoc.render(0, tableSpec.toJson))
  }
}
