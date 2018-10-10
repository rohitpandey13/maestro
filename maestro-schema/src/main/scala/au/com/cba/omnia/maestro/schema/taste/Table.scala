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

import au.com.cba.omnia.maestro.schema._
import au.com.cba.omnia.maestro.schema.pretty._


/** Accumulated taste information for an entire table. */
case class Table(
  maxHistSize: Int,
  rowTastes:   mutable.Map[Int, Row]) {

  /** Convert a TableTaste to JSON, attaching field names and storage types
   *  if we have them. The maps contain the field names and types for rows
   *  with the given number of fields. */
  def toJson(
    fieldNames:   Map[Int, List[String]],
    storageTypes: Map[Int, List[String]])
    : JsonDoc =

    JsonMap(
      rowTastes.toList.map { case (_, rt) => {

        // Get the field names for rows of this length, if we have them.
        val oNames:   Option[List[String]] =
          fieldNames  .get(rt.fieldTastes.length)

        // Get the storage types for rows of this length, if we have them.
        val oStorage: Option[List[String]] =
          storageTypes.get(rt.fieldTastes.length)

        ("columns", rt.toJson(oNames, oStorage))
      }},
      true)
}


/** Functions concerning TableTastes. */
object Table {

  /** Create a new, empty TableTaste. */
  def empty(maxHistSize: Int): Table =
    Table(maxHistSize, mutable.Map())


  /** Destructively accumulate a new row into the TableTaste.
   *  If this was the first row added then also return the taste wrapped in
   *  a Some, otherwise None. */
  def accumulateRow(tt: Table, str: String): Option[Table] = {

    // Remember whether the initial map was empty.
    // If it is then this is the first row that we're processing,
    // so we want to pass on our (mutable) thread-local map to the reducer.
    val init    = tt.rowTastes.size == 0

    // Split the input row into individual fields.
    val fields  = str.split('|')

    // Get the array that that contains classifier counts for rows with this
    // many fields, and add the classifier counts for this row to it.
    tt.rowTastes.get(fields.length) match {

      // We don't yet have an array of classifier counts for rows with this
      // many fields, so make one now.
      case None      => {
        val rowTaste  = Row.classify(tt.maxHistSize, fields)
        tt.rowTastes += ((fields.length, rowTaste))
      }

      // We already have an array of classifier counts for rows with this
      // many fields, so acumulate the new counts into it.
      case Some(rowTaste) => {
        Row.accumulate(rowTaste, fields)
      }
    }

    if(init)  Some(tt)
    else      None
  }


  /** Get all the possible classifications for a given field. */
  def classifyField(s: String): Array[Int] =
    Classifier.all
      .map {_.likeness(s)}
      .map {like => if (like >= 1.0) 1 else 0}


  /** Combine the information in two TableTastes to produce a new one. */
  def combine(tt1: Table, tt2: Table): Table = {

    val tt3: Table =
      empty(tt1.maxHistSize)

    for ((num, rt1) <- tt1.rowTastes) {
      if (tt3.rowTastes.isDefinedAt(num))
            tt3.rowTastes += ((num, Row.combine(tt3.rowTastes(num), rt1)))
      else  tt3.rowTastes += ((num, rt1))
    }

    for ((num, rt2) <- tt2.rowTastes) {
      if (tt3.rowTastes.isDefinedAt(num))
            tt3.rowTastes += ((num, Row.combine(tt3.rowTastes(num), rt2)))
      else  tt3.rowTastes += ((num, rt2))
    }

    tt3
  }

}
