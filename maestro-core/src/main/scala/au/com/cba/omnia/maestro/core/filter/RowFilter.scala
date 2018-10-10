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

package au.com.cba.omnia.maestro.core.filter

/**
  * Filter for individual rows of data.
  * 
  * Use this to remove rows that should not be processed for example header or trailer records.
  * Runs across a list of string and returns `Some` subset of the list if the filter is satisfied.
  * Otherwise returns `None`.
  */
case class RowFilter(run: List[String] => Option[List[String]]) {
  /**
    * Combines to filters, one after the other.
    * 
    * The input is processed sequentially. First `this` filter is applied to the input and then the
    * `f` is applied to the output of running `this`.
    */
  def and(f: RowFilter): RowFilter = RowFilter(l => run(l).flatMap(f.run))

  /**
    * Combines to filters, one after the other.
    * 
    * The input is processed sequentially. First `this` filter is applied to the input and then the
    * `f` is applied to the output of running `this`.
    */
  def &&&(f: RowFilter): RowFilter = and(f)
}

object RowFilter {
  /** Passes through everything.*/
  def keep: RowFilter =
    RowFilter(Some.apply)

  /**
    * Only keeps rows where the first field matches `include`.
    * 
    * It also removes the first field after filtering.
    */
  def byRowLeader(include: String): RowFilter =
    RowFilter(row => if (row.headOption.exists(_ == include)) Some(row.tail) else None)


  /** Drops the last field. */
  def init: RowFilter = RowFilter(l => 
    if (l.isEmpty) None
    else           Option(l.init)
  )

  /**
    * Drops the field for a particular index
    *
    * Index value starts from 0
    */
  def dropI(i: Int): RowFilter = RowFilter(row => Some(row.take(i) ++ row.drop(i+1)))
}
