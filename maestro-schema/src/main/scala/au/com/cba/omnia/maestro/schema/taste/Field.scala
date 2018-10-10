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


/** Classifier counts for a single field in a row. */
case class Field(
  clasCounts: Array[Int],
  histField:  Sample) {

  /** Convert the FieldTaste to JSON. */
  def toJson(
      name:    Option[String],
      storage: Option[String]): JsonDoc = {

    // Histogram of how many values in the column matched each classifier.
    val histClas: Histogram = 
      Histogram(Classifier.all .zip (clasCounts) .toMap)

    JsonMap(
      (name    match { case None => List(); case Some(s) => 
        List(("name",    JsonString(s))) }) ++
      (storage match { case None => List(); case Some(s) => 
        List(("storage", JsonString(s))) }) ++
      List(
        ("classifiers", histClas .toJson),
        ("sample",      histField.toJson)),
      true)
  }
}


object Field {

  /** Create a new, empty FieldTaste. */
  def empty(maxHistSize: Int): Field = {
    val clasCounts: Array[Int] = 
      Classifier.all.map { _ => 0 }

    val histField: Sample = 
      Sample.empty(maxHistSize)

    Field(clasCounts, histField)
  } 


  /** Compute the possible classifications for a given field. */
  def classify(maxHistSize: Int, str: String): Field = {
    val clasCounts: Array[Int] = 
      Classifier.all
        .map {_.likeness(str)}
        .map {like => if (like >= 1.0) 1 else 0}

    val histField: Sample = 
      Sample.empty(maxHistSize)

    Field(clasCounts, histField)
  }


  /** Given a FieldTaste, and a new field value, destructively update
   * the counters in the FieldTaste. */
  def accumulate(ft: Field, str: String): Unit = {
   
    // Update the classifier counts.
    for (c <- 0 to Classifier.all.length - 1) {
      val clas    = Classifier.all(c)
      val n: Int  = if (clas.likeness(str) >= 1.0) 1 else 0
      ft.clasCounts(c) += n
    }

    // Update the histogram.
    Sample.accumulate(ft.histField, str)
  }


  /** Combine the information in two FieldTastes to produce a new one. */
  def combine(ft1: Field, ft2: Field): Field = {

    val clasCountsX =
      ft1.clasCounts .zip (ft2.clasCounts) .map { 
        case (x1, x2) => x1 + x2 }

    val histFieldX =
      Sample.combine(ft1.histField, ft2.histField)

    Field(clasCountsX, histFieldX)
  }
}
