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

package au.com.cba.omnia.maestro.scalding

import com.twitter.scalding.StatKey

import cascading.flow.StepCounters

/** Common counter keys. */
object StatKeys {
  /** Key for number of tuples written. */
  val tuplesWritten = StatKey(StepCounters.Tuples_Written.toString, "cascading.flow.StepCounters")

  /** Key for number of tuples read. */
  val tuplesRead    = StatKey(StepCounters.Tuples_Read.toString, "cascading.flow.StepCounters")

  /** Key for number of tuples trapped. */
  val tuplesTrapped = StatKey(StepCounters.Tuples_Trapped.toString, "cascading.flow.StepCounters")

  /** Key for number of tuples filtered out by using a filter. */
  val tuplesFiltered = StatKey("tuples_filtered", "maestro")

  /** Key for number of tuples actually output by `view` or `viewHive`.
    * Alas, using [[tuplesWritten]] incorrectly includes internal writes when there are multiple steps,
    * since currently scalding only provides access to the total counts for a whole flow.
    */
  val tuplesOutput = StatKey("tuples_output", "maestro")
}
