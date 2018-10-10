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
package taste

import scala.collection._

import au.com.cba.omnia.maestro.schema.pretty._


/** A SampleMap is used to build a historgram of the number of times we've seen
 * each string in a set of strings. The number of strings we're prepared to
 * track is limited to some fixed amount. */
case class Sample(
    maxSize:   Int,           // Maximum size of the histogram.
    spilled:   Array[Int],    // Count of strings that wouldn't fit in this histogram.
    histogram: mutable.Map[String, Int]) // Histogram of times we've seen each string.
{

  /** Pretty print a column sample as JSON lines. */
  def toJson: JsonDoc =
    JsonMap(List(
      ("maxSize",   JsonNum(maxSize)),
      ("spilled",   JsonNum(spilled(0))),
      ("histogram", toJsonHistogram)))


  /** Pretty print the sample histogram.
   *  We print it sorted, so the most frequently occurring strings are listed
   *  first */
  def toJsonHistogram: JsonDoc =
    JsonMap(
      histogram
        .toList
        .sortBy { _._2 }
        .reverse
        .map    { case (k, v) => (k, JsonNum(v)) },
      false)
}


object Sample {

  /** Create a new, empty SampleMap */
  def empty(maxSize: Int): Sample =
    Sample(maxSize, Array(0), mutable.Map())

  /** Accumulate a new string into a SampleMap */
  def accumulate(smap: Sample, str: String): Unit = {

    // If there is already an entry in the map for this string then
    // we can increment that.
    if (smap.histogram.isDefinedAt(str)) {
      // assigning to m suppresses a Scala warning: "discarded non-Unit value"
      //  the map is mutable, so we don't need the new reference to it
      val m = smap.histogram += ((str, smap.histogram(str) + 1))
    }

    // If there is still space in the map then add a new entry.
    else if (smap.histogram.size < smap.maxSize - 1) {
      // assigning to m suppresses a Scala warning: "discarded non-Unit value"
      //  the map is mutable, so we don't need the new reference to it
      val m = smap.histogram += ((str, 1))
    }

    // Otherwise remember that we had to spill this string.
    else
      smap.spilled(0) += 1
  }


  /** Combine the information in two SampleMaps, to produce a new one. */
  def combine(sm1: Sample, sm2: Sample): Sample = {

    // Accumulate the result into this empty SampleMap.
    val sm3: Sample
      = empty(sm1.maxSize)

    // Combine the histograms.
    for ((str, count1) <- sm1.histogram) {
      if (sm3.histogram.isDefinedAt(str))
            sm3.histogram += ((str, sm3.histogram(str) + count1))
      else  sm3.histogram += ((str, count1))
    }

    for ((str, count2) <- sm1.histogram) {
      if (sm3.histogram.isDefinedAt(str))
            sm3.histogram += ((str, sm3.histogram(str) + count2))
      else  sm3.histogram += ((str, count2))
    }

    // Combine the spill counters.
    sm3.spilled(0)  = sm1.spilled(0) + sm2.spilled(0)

    sm3
  }

}
