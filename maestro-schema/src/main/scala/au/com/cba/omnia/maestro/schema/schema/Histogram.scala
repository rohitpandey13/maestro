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


/** Histogram of how many values in a column match each Classifier
 *
 *  @param counts How many values match each classifier.
 */
case class Histogram(counts: Map[Classifier, Int]) {

  /** Yield the number of classifiers in the histogram. */
  def size: Int =
    this.counts.size


  /** Pretty print the histogram as a String. */
  def pretty: String = {

    val strs =
      sorted
        // Pretty print with the count after the classifier name.
        .map      { case (c, i)   => c.name + ":" + i.toString }

    // If there aren't any classifications for this field then
    // just print "-" as a placeholder.
    if (strs.length == 0) "-"
    else strs.mkString(", ")
  }


  /** Extract the histogram with the classifiers in the standard sorted
      order. Don't return classifiers with zero counts. */
  def sorted: List[(Classifier, Int)] =
    counts
      .toList

      // Drop classifications that didn't match.
      .filter   { case (c, i)   => i > 0 }

      // Sort classifications so we get a deterministic ordering
      // between runs.
      .sortWith { (x1, x2) => x1._1.sortOrder < x2._1.sortOrder }


  /** Convert the histogram to JSON. */
  def toJson: JsonDoc =
    JsonMap(sorted
      .map  { case (c, i) => (c.name, JsonNum(i)) })
}
