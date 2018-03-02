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
package jobs

import scala.io.Source

import com.twitter.scalding._

import au.com.cba.omnia.maestro.schema.taste._
import au.com.cba.omnia.maestro.schema.hive.Input
import au.com.cba.omnia.maestro.schema.pretty._


/** Job to scan through data from a input table and build a histogram
  * of how many values in each column match each possible classifier. */
class Taste(args: Args) 
  extends Job(args) {

  // Input HDFS path for input data files.
  val fileInputData   = args("in-hdfs-data")
  val filesInputData  = Input.list(fileInputData)

  val pipeInput: TypedPipe[String] =
    TypedPipe.from(MultipleTextLineFiles(filesInputData.map{_.toString} :_*))

  // Optional Input names file. 
  // This is a text file with one line for each column in the table.
  // The first word on the line is the column name, and the second is the Hive
  // storage type.
  val fileInputNames  = args.optional("in-local-names")
  val namesStorage: List[List[String]] =
    fileInputNames match {
      case None       => List()
      case Some(file) => 
        Source.fromFile(file)
         .getLines .map { w => w.split("\\s+").toList }
         .toList
    }

  // Map of row length to field names for rows of that length.
  // We only track one row length at the momement.
  val fieldNames:   Map[Int, List[String]] = {
    val names   = namesStorage.map(_(0)).toList
    List((names.length, names)).toMap 
  }

  // Map of row length to storage types for rows of that length.
  // We only track one row length at the moment.
  val fieldStorage: Map[Int, List[String]] = {
    val storage = namesStorage.map(_(1)).toList
    List((storage.length, storage)).toMap
  }

  // Output Destination HDFS path for taste result.
  val fileOutputTaste = args("out-hdfs-taste")
  val pipeOutputTaste = TypedTsv[String](fileOutputTaste)

  // (thread local)
  // Holds the counts of how many values match each classifier.
  // Each mapper job updates its own mutable map with the classifier counts
  // gained from its chunk of data. The maps from each job are then combined
  // in a single reducer.
  val taste: Table = 
    Table.empty(100)

  // Read lines from the input files and accumulate them into a TableTaste.
  val pTastes: TypedPipe[Table] =
    pipeInput

      // Build a map of which types are matched by each line.
      // The first time the worker is called it passes on a reference to the
      // mutable counts map. For each successive time, it destructively accumulates
      // the new counts into the existing map.
      .flatMap { line: String => 
        Table.accumulateRow(taste, line) }

      // Sum up the counts from each node on a single reducer.
      .groupAll
      .reduce (Table.combine)
      .values

    // Show the classifications in a human readable format.
    pTastes
      .map { tasteFinal: Table => 
        JsonDoc.render(0, tasteFinal.toJson(fieldNames, fieldStorage)) }

      // Write the counts out to file.
      .write(pipeOutputTaste)
}

