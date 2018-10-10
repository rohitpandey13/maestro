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

/** Companion object for Schemas. */
object Schema {

  /** Show the classification counts for row of the table.
   *  The counts for each field go on their own line. */
  def showCountsRow(
    classifications: Array[Classifier],
    counts:          Array[Array[Int]])
    : String =
    counts
      .map { counts => showCountsField(classifications, counts) + ";\n" }
      .mkString


  /** Show the classification counts for a single field,
   *  or '-' if there aren't any. */
  def showCountsField(
    classifications: Array[Classifier],
    counts:          Array[Int])
    : String =
    Histogram(Classifier.all .zip (counts) .toMap).pretty
}
