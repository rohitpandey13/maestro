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

import au.com.cba.omnia.maestro.schema.pretty._


/** Classifier counts for a row sort, with some fixed number of fields. */
case class Row(
  fieldTastes: Array[Field]) {

  /* Convert the Row Taste to JSON, attaching field names and storage
   * types if we have them. */
  def toJson(
    fieldNames:   Option[List[String]],
    storageTypes: Option[List[String]]): JsonDoc =
    JsonList(
      fieldTastes.toList.zipWithIndex.map { case (ft: Field, ix: Int) =>
        ft.toJson(fieldNames.map(_(ix)), storageTypes.map(_(ix))) },
      true)
}


object Row {

  /** Create a new, empty RowTaste. */
  def empty(maxHistSize: Int, fieldNum: Int): Row = {

    val fieldTastes: Array[Field] =
      (for  { i <- 0 to fieldNum - 1 }
       yield Field.empty(maxHistSize))
        .toArray

    Row(fieldTastes)
  }


  /** Compute the possible classifications for the given fields. */
  def classify(maxHistSize: Int, strs: Array[String]): Row =
    Row(strs.map {str => Field.classify(maxHistSize, str)})


  /** Destructively accumulate field strings into the RowTaste.
   *  If the number of fields is different than the existing RowTaste
   *  then do nothing  */
  def accumulate(rt: Row, strs: Array[String]): Unit = {
    if (rt.fieldTastes.length != strs.length)
      ()
    else for (f <- 0 to strs.length - 1) {
      Field.accumulate(rt.fieldTastes(f), strs(f))
    }
  }


  /** Combine the information in two RowTastes to produce a new one. */
  def combine(rt1: Row, rt2: Row): Row = {

    val fieldTastesX: Array[Field] =
      rt1.fieldTastes .zip (rt2.fieldTastes) .map {
        case (ft1, ft2) => Field.combine(ft1, ft2) }

    Row(fieldTastesX)
  }
}
